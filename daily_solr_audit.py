#!/usr/bin/python

import params as p
from functions import *
import datetime
import os
import re
import sys
import socket

# Get current working directory and script name
cwd = socket.getfqdn() + os.path.abspath(sys.argv[0])

# Get command line options if any
arg_dict = processArgs(sys.argv[1:])

# For multiple sources, Solr query is executed only on first iteration if 
# sources have the same query URL
solr_query_done = False
solr_query_url = None

# Load settings from params file
for source_settings in p.config:
    source = Datasource(source_settings)
    # Process command line arguments
    if arg_dict['source']:
        if arg_dict['source'].lower() != source.name.lower():
            continue
    if arg_dict['resend']:
        # resend notification emails for -r date
        resendEmail(source.name, source.email_filename, \
                        source.email_server, arg_dict['resend'], \
                        source.email_path, source.archive_path)
        continue
    if arg_dict['date']:
        source.alternate_date = setDate(arg_dict['date'])
    if arg_dict['viz']:
        source.viz_output = arg_dict['viz']

    # Filename addition to indicate alternate date if set
    if source.alternate_date is not None:
        add_alt = '.' + ''.join(getDate(source.alternate_date)) + 'ALT'
    else:
        add_alt = ''

    # Check that directories indicated in params exist, and create if necessary
    paths = set([source.log_path, source.stat_path, source.data_path, \
                source.archive_path, source.email_path, source.viz_output_path])
    for path in paths:
        confirmDir(path)
    # File management - archive and delete older files
    path_dict = getPaths(source.log_path, source.stat_path, source.data_path, \
                             source.email_path, source.viz_output_path)
    doFileRotation(source.rotation_data, path_dict, source.archive_path, \
                       source.email_server, source.email_sender, \
                       source.email_recipients)

    # If log and/or stats files do not exist or are empty, create files and/or 
    # write header line.
    log_header = ['timestamp', 'audit date', 'filename']
    stat_header = ['timestamp', 'audit date', 'source', 'action', 'extract', 
                   'load', 'ERRORS']
    source.log_file = substituteDate(None, source.log_file)
    source.stat_file = substituteDate(None, source.stat_file)
    log = source.log_path + source.log_file
    stat = source.stat_path + source.stat_file
    output_files = {log: log_header, stat: stat_header}
    for filename, header in output_files.iteritems():
        with open(filename, 'a+') as fh:
            if not fh.readline():
                fh.write('\t'.join(header) + '\n')
    # Set default audit status for notification email subject line
    status = 'OK'
    # Get date of audit (default is today, set params.alternate_date or use 
    # command line option -d YYYYMMDD to override)
    audit_date = getDateString(source.alternate_date)
    # Set filename patterns to select files for audit date
    regex_dict = dict((k, substituteDate(audit_date, v)) \
                  for (k, v) in source.input_filenames.iteritems())
    # Create lists to store audit date's filenames
    file_dict = dict((k, []) for k in regex_dict.iterkeys())
    # Get input files already audited today
    # Returns dictionary of empty values if alternate date is set
    processed_files = getProcessedFiles(source.log_path, source.log_file, \
                                            source.alternate_date, regex_dict)
    # Get matching filenames and add to lists
    # f=filename, k=key in regex_dict and file_dict, v=regex patt in regex_dict
    for f in os.listdir(source.input_path):
        for k, v in regex_dict.iteritems():
            if re.match(v, f) and f not in processed_files[k]:
                file_dict[k].append(f)
    # Log file names identified; if no files to process, write to output if
    # indicated in params and proceed to next source
    timestamp = str(datetime.datetime.now()).split('.')[0]
    input_found = False
    log_data = [log_header]
    for action, files in file_dict.iteritems():
        # If files were identified, write to log
        if files:
            input_found = True
            for filename in files:
                log_data.append([timestamp, audit_date, filename])
        # If no files were identified, write to log if conditions met
        else:
            # Write to log if alternate_date is set to a value
            if source.alternate_date is not None:
                status = 'NO FILES FOUND'
                error_msg = 'no %s %s files found' % (source.name, action)
            # Write to log if log_if_none is set to True
            elif source.log_if_none is True:
                status = 'NO NEW FILES FOUND'
                error_msg = 'no new %s %s files found' % (source.name, action)
            # Otherwise, continue to next action with no output
            else:
                continue
            log_data.append([timestamp, audit_date, error_msg.upper()])
    if input_found == False:
        # Write to log and email if condition met; otherwise, no output
        if source.log_if_none is True:
            # Write to log, omitting headers
            appendOutput(log_data[1:], source.log_path, source.log_file)
            # Write and send email notification
            subject, report = formatReport(audit_date, source.name, status, \
                                               cwd, log=log_data)
            msg = writeEmail(source.email_server, source.email_sender, \
                           source.email_recipients, subject, report)
            writeEmailToFile(source.log_path, source.email_filename, '.email', \
                                 msg)
        continue

    # Get bib ids of records added, suppressed, deleted from extract files
    bibs_by_file = {}
    bibs_by_file['add'] = getBibsFromMarc(source.input_path, file_dict['add'], \
                                              mfhd=source.skip_MFHDs)
    bibs_by_file['suppress'] = getBibsFromText(source.input_path, \
                                                   file_dict['suppress'], \
                                                   source.bib_pattern)
    bibs_by_file['delete'] = getBibsFromText(source.input_path, \
                                                 file_dict['delete'], \
                                                 source.bib_pattern)
    # Get bib ids of records previously processed
    # Returns empty dictionary if alternate date is set
    processed_bibs = getProcessedBibs(source.input_path, processed_files, \
                                          file_dict, source.skip_MFHDs, \
                                          source.alternate_date, \
                                          source.bib_pattern)
    # Remove bib ids from current sets
    # Consolidate sets for comparison to Solr
    source_bibs = {}
    for action in bibs_by_file.iterkeys():
        bibs_by_file[action] = removeProcessedBibs(bibs_by_file[action], \
                                                       processed_bibs[action])
        source_bibs[action] = combineSets(bibs_by_file[action])
    # Query Solr if not already done with same URL for previous source
    if not solr_query_done or solr_query_url != source.solr_url:
        solr_data = querySolr(source.solr_url)
        solr_bibs = set(solr_data.keys())
        solr_query_done = True
        solr_query_url = source.solr_url

    # Create dictionaries for results
    solr_success = (dict((k, set()) for k in file_dict.iterkeys()))
    solr_error = (dict((k, set()) for k in file_dict.iterkeys()))

    # Check whether bibs in add lists are present and timestamped with audit
    # date
    for bib in source_bibs['add']:
        # success
        if bib in solr_bibs and solr_data[bib] >= audit_date:
            solr_success['add'].add(bib)
        # error
        else:
            solr_error['add'].add(bib)

    # Check whether bibs in suppressed/deleted lists are not present in Solr
    # success
    solr_success['suppress'] = source_bibs['suppress'].difference(solr_bibs)
    solr_success['delete'] = source_bibs['delete'].difference(solr_bibs)
    # error
    solr_error['suppress'] = source_bibs['suppress'].intersection(solr_bibs)
    solr_error['delete'] = source_bibs['delete'].intersection(solr_bibs)

    # Dictionary of results, folding suppressions into deletions
    solr_results = {
        'solr_added': solr_success['add'],
        'solr_deleted': solr_success['suppress'].union(solr_success['delete']),
        'solr_not_added': solr_error['add'],
        'solr_not_deleted': solr_error['suppress'].union(solr_error['delete'])
        }
    # Dictionary of headers for data files
    data_headers = {
        'solr_added': ['bib_id', 'solr_last_updated'],
        'solr_deleted': ['bib_id'],
        'solr_not_added': ['bib_id', 'solr_last_updated'],
        'solr_not_deleted': ['bib_id', 'solr_last_updated']}

    # Write bibs processed to data files corresponding to source/action/outcome
    # Determine output based on params settings and audit results
    # add_alt is empty string if source.alternate_date is None
    data_files = dict((k, (getFileNameTimestamp(v[0] + add_alt, '.bib.txt'), \
                           v[1])) \
                          for (k, v) in source.data_filenames.iteritems() \
                          if solr_results[k])
    writeBibsToLogs(solr_data, solr_results, source.data_path, data_files, \
                        data_headers)

    # Generate audit stats for source
    if source.alternate_date is not None:
        alt = 'ALT'
    else:
        alt = ''
    stat_data = [
        stat_header,
        [timestamp, audit_date + alt, source.name, 'ADD', 
         str(len(source_bibs['add'])), 
         str(len(solr_results['solr_added'])), 
         str(len(solr_results['solr_not_added']))],
        [timestamp, audit_date + alt, source.name, 'DEL', 
         str(len(source_bibs['suppress'].union(source_bibs['delete']))), 
         str(len(solr_results['solr_deleted'])), 
         str(len(solr_results['solr_not_deleted']))]
    ]

    # Update audit status for notification email subject line
    # Action unsuccessful
    if solr_results['solr_not_added'] or solr_results['solr_not_deleted']:
        status = 'REVIEW'
    # Extract files found but empty
    if len(source_bibs['add']) == 0 or \
            len(source_bibs['suppress'].union(source_bibs['delete'])) == 0:
        status = 'REVIEW'

    # Write to log of files processed and cumulative stats file
    # Omit headers
    appendOutput(log_data[1:], source.log_path, source.log_file)
    appendOutput(stat_data[1:], source.stat_path, source.stat_file)

    # Construct notification email with attachments, if any
    # Send email to recipients in params
    # Save email to backup file (to resend if necessary)
    subject, report = formatReport(audit_date, source.name, status, cwd, \
                                       log=log_data, stats=stat_data)
    files_to_attach = [source.data_path + f[0] for f in data_files.values() \
                           if f[1] == True]
    msg = writeEmail(source.email_server, source.email_sender, \
                         source.email_recipients, subject, report, \
                         attach=files_to_attach)
    writeEmailToFile(source.log_path, source.email_filename + add_alt, \
                         '.email', msg)
    # Add stats by extract file to database for use by visualization service
    # If filename is already in database, new data will overwrite old
    if source.viz_output == 'on':
        # Prepare data
        viz_stat_data = {}
        for action, file_sets in bibs_by_file.iteritems():
            for f, bib_set in file_sets.iteritems():
                viz_stat_data[f] = {action: {}}
                viz_stat_data[f][action]['extract'] = len(bib_set)
                viz_stat_data[f][action]['load'] = \
                    len(bib_set.intersection(solr_success[action]))
                viz_stat_data[f][action]['error'] = \
                    len(bib_set.intersection(solr_error[action]))
        # Structure data for SQL query
        viz_sql_data = {}
        viz_file_output = {}
        for action, filenames in file_dict.iteritems():
            for f in filenames:
                viz_sql_data[f] = {
                    'audit_date': audit_date, 
                    'timestamp': timestamp,
                    'resource': source.name,
                    'action': action,
                    'extract': viz_stat_data[f][action]['extract'],
                    'error': viz_stat_data[f][action]['error'],
                    'load': viz_stat_data[f][action]['load'],
                    'filename': f}
        # Write to database and backup files
        queries = writeToDatabase(source.viz_output_db, viz_sql_data)
        writeSQLToFile(source.viz_output_path, source.viz_output_filename, \
                           add_alt, '.viz.sql', queries)
