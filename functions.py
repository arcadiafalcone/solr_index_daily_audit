#!/usr/bin/python

class Datasource:
    """Interpret source settings from params."""
    def __init__(self, data):
        self.name = data['name']
        self.alternate_date = data['alternate_date']
        self.input_path = data['input_path']
        self.input_filenames = data['input_filenames']
        self.skip_MFHDs = data['skip_MFHDs']
        self.bib_pattern = data['bib_pattern']
        self.solr_url = data['solr_url']
        self.log_path = data['log_path']
        self.log_file = data['log_file']
        self.log_if_none = data['log_if_none']
        self.stat_path = data['stat_path']
        self.stat_file = data['stat_file']
        self.data_path = data['data_path']
        self.data_filenames = data['data_filenames']
        self.archive_path = data['archive_path']
        self.email_path = data['email_path']
        self.email_filename = data['email_filename']
        self.email_recipients = data['email_recipients']
        self.email_server = data['email_server']
        self.email_sender = data['email_sender']
        self.viz_output = data['viz_output']
        self.viz_output_db = data['viz_output_db']
        self.viz_output_path = data['viz_output_path']
        self.viz_output_filename = data['viz_output_filename']
        self.rotation_data = data['rotation_data']

def getDate(alternate_date):
    """Process current or alternate date for use as audit date."""
    import datetime
    if alternate_date is not None:
        d = [int(x) for x in alternate_date.split('-')]
        n = datetime.datetime(d[0], d[1], d[2])
    else:
        n = datetime.datetime.now()
    year = str(n.year)
    month = '%02d' % n.month
    day = '%02d' % n.day
    return year, month, day

def getDateString(audit_date):
    """Return date matching the timestamp format in Solr query results."""
    year, month, day = getDate(audit_date)
    datestring = '-'.join([year, month, day])
    return datestring

def substituteMultiple(sub_dict, text):
    """Perform multiple substitutions in string based on dictionary.

    Dictionary contains regex: sub.
    """
    import re
    regex = re.compile(r'|'.join(sub_dict.keys()))
    return regex.sub(lambda x: sub_dict[x.string[x.start():x.end()]], text)

def substituteDate(audit_date, text):
    """Substitute audit date for placeholders in filenames."""
    year, month, day = getDate(audit_date)
    date_dict = {'YYYY': year, 'MM': month, 'DD': day}
    return substituteMultiple(date_dict, text)

def querySolr(solr_url):
    """Query the Solr index based on parameters in solr_url."""
    import urllib2
    solr_data = {}
    req = urllib2.Request(solr_url)
    response = urllib2.urlopen(req)
    header = response.readline()
    for line in response:
        fields = line.rstrip('\n').split(',')
        solr_data[fields[0]] = fields[1].split('T')[0]
    return solr_data

def appendOutput(data, path, filename):
    """Append output to file."""
    with open(path + filename, 'a+') as fh:
        for line in data:
            output = '\t'.join(line) + '\n'
            fh.write(output)

def getBibsFromMarc(path, marc_file_list, mfhd=False):
    """Analyze .mrc file(s) and extract bib ids by file. 

    Skip MFHDs if mfhd=True. Return values as dictionary of sets 
    {filename: set(bibs)}.
    """
    from pymarc import MARCReader, Record, Field
    bib_set_dict = dict()
    for f in marc_file_list:
        bib_set_dict[f] = set()
        reader = MARCReader(file(path + f), to_unicode=True)
        for record in reader:
            if mfhd and not record.get_fields('004'):
                bib_set_dict[f].add(record['001'].value())
            elif not mfhd:
                bib_set_dict[f].add(record['001'].value())
    return bib_set_dict

def getBibsFromText(path, text_file_list, bib_pattern):
    """Process text file(s) listing bib ids. 

    Return values as dictionary of sets {filename: set(bibs)}.
    """
    import re
    patt = r'{0}'.format(bib_pattern)
    bib_set_dict = dict()
    for f in text_file_list:
        bib_set_dict[f] = set()
        with open(path + f) as fh:
            for line in fh:
                if re.match(patt, line):
                    bib_set_dict[f].add(line.rstrip('\r\n'))
    return bib_set_dict

def getProcessedFiles(source_log_path, source_log_file, source_alt_date, \
                          regex_dict):
    """Extract names of files already processed on current date from log.

    Return an empty dictionary if alternate date is set in params.py or as a
    command-line option.
    """
    import re
    today = getDateString(None)
    processed_files = dict((k, []) for k in regex_dict.iterkeys())
    if source_alt_date is not None:
        return processed_files
    with open(source_log_path + source_log_file) as fh:
        for line in fh:
            fields = line.rstrip('\n').split('\t')
            try:
                if fields[1] == today:
                    for k, v in regex_dict.iteritems():
                        if re.match(v, fields[2]):
                            processed_files[k].append(fields[2])
            except IndexError:
                pass
    return processed_files

def getProcessedBibs(input_data_path, processed_files, file_dict, \
                         source_skip_mfhds, source_alt_date, bib_pattern):
    """If processed files are identified for the current date, omit the records
    in those files from subsequent record sets for same action at runtime.

    Return an empty dictionary if alternate date is set in params.py or as a
    command-line option.
    """
    processed_bibs = dict((k, set()) for k in file_dict.iterkeys())
    if source_alt_date is not None:
        return processed_bibs
    for k in file_dict:
         if processed_files[k]:
             if processed_files[k][0].endswith('mrc'):
                 bib_set_dict = getBibsFromMarc(input_data_path, \
                                                    processed_files[k], \
                                                    mfhd=source_skip_mfhds)
             elif processed_files[k][0].endswith('txt'):
                 bib_set_dict = getBibsFromText(input_data_path, \
                                                    processed_files[k], \
                                                    bib_pattern)
             else:
                 bib_set_dict = {}
             for bib_set in bib_set_dict.itervalues():
                 processed_bibs[k].update(bib_set)
    return processed_bibs

def removeProcessedBibs(bib_set_dict, processed_bibs_list):
    """Remove bib ids already processed from current set, and dedupe multiple 
    sets for the same action against each other."""
    processed_bibs_set = set(processed_bibs_list)
    for f in bib_set_dict.iterkeys():
        bib_set_dict[f].difference_update(processed_bibs_set)
    if len(bib_set_dict) > 1:
        done_set = set()
        order = sorted(bib_set_dict.iterkeys())
        for f in order:
            bib_set_dict[f].difference_update(done_set)
            done_set.update(bib_set_dict[f])
    return bib_set_dict

def combineSets(set_dict):
    """Combine dictionary values into a single set."""
    new_set = set()
    for old_set in set_dict.itervalues():
        new_set.update(old_set)
    return new_set

def writeBibsToLogs(solr_data, solr_results, data_path, data_files, \
                        data_headers):
    """Generate files listing bib ids associated with a particular action and 
    status, and timestamp if applicable."""
    for key, filename in data_files.iteritems():
        with open(data_path + filename[0], 'w') as fh:
            fh.write('\t'.join(data_headers[key]) + '\n')
            for bib in solr_results[key]:
                try:
                    fh.write(bib + '\t' + solr_data[bib] + '\n')
                except KeyError:
                    fh.write(bib + '\t\n')

def getColumnWidths(data, padding=3):
    """Get column widths for writing tabular data to output.

    Assumes all data rows are same length as first row.
    """
    column_widths = []
    for i in range(0, len(data[0])):
        w = max(len(row[i]) for row in data) + padding
        column_widths.append(w)
    return column_widths

def formatTabularData(data, padding=3):
    """Format data from nested lists into columns for output."""
    data_formatted = []
    col_widths = getColumnWidths(data, padding)
    for row in data:
        row_formatted = []
        for width, entry in zip(col_widths, row):
            row_formatted.append('{0:{width}}'.format(entry, width=width))
        row_formatted[-1] = row_formatted[-1].rstrip()
        data_formatted.append(''.join(row_formatted))
    data_output = '\n'.join(data_formatted)
    return data_output

def formatReport(audit_date, source_name, status, cwd, **kwargs):
    """Format audit summary for notification email."""
    footer = 'Generated from %s' % cwd
    today = getDateString(None)
    if audit_date != today:
        subject = 'Solr audit for ALT DATE %s: %s %s' \
            % (audit_date, source_name, status)
    else:
        subject = 'Solr audit for %s: %s %s' % (audit_date, source_name, status)
    source_heading = '%s\n' % (source_name.upper())
    # Define keyword arguments
    log = kwargs.get('log', None)
    stats = kwargs.get('stats', None)
    # log data
    log_data_output = formatTabularData(log, padding=5)
    # stats data
    try:
        stats_data_output = formatTabularData(stats)
        report = '\n'.join([source_heading, log_data_output, '', \
                                stats_data_output, '\n', footer])
        return subject, report
    except (ValueError, TypeError):
        report = '\n'.join([source_heading, log_data_output, '\n', footer])
        return subject, report

def writeEmail(server, sender, recipients, subject, report, **kwargs):
    """Construct and send notification email with data file attachments, if any.

    Server, sender, and recipients are derived from params.
    """
    import smtplib
    from os.path import basename
    from email.utils import COMMASPACE, formatdate
    from email.mime.text import MIMEText
    from email.mime.application import MIMEApplication
    from email.mime.multipart import MIMEMultipart
    from email.message import Message
    msg = MIMEMultipart()
    msg['from'] = sender
    msg['to'] = COMMASPACE.join(recipients)
    msg['date'] = formatdate(localtime=True)
    msg['subject'] = subject
    html_report = makeHTML(report)
    msg_body = MIMEMultipart('alternative')
    msg_body.attach(MIMEText(report, 'plain'))
    msg_body.attach(MIMEText(html_report, 'html'))
    msg.attach(msg_body)
    try:
        attach_filenames = kwargs.get('attach')
        for f in attach_filenames:
            with open(f, 'r') as fh:
                attachment = MIMEText(fh.read())
                attachment.add_header('Content-Disposition', 'attachment', \
                                          filename=basename(f))
                msg.attach(attachment)
    except (ValueError, TypeError):
        pass
    smtp = smtplib.SMTP(server)
    smtp.sendmail(sender, recipients, msg.as_string())
    smtp.close()
    return msg

def makeHTML(report):
    """Create HTML version of report with monospace font."""
    import re
    report_n = re.sub(r'\n', '<br>', report)
    report_s = re.sub(r'\s', '&nbsp;', report_n)
    report_d = re.sub('<br><br>', r'<br>\n<br>', report_s)
    html_report = """\
<html>
 <head></head>
 <body>
  <p style="font-family:courier">%s</p>
 </body>
</html>""" % report_d
    return html_report
    
def writeEmailToFile(email_path, email_filename, extension, msg):
    """Write the email object to a file as a string.

    This file can be re-sent as email with the -r YYYYMMDD command line option.
    """
    filename = getFileNameTimestamp(email_filename, extension)
    with open(email_path + filename, 'w') as fh:
        fh.write(msg.as_string())

def getFileNameTimestamp(filename_base, file_extension):
    """Construct filename using text base set in params and timestamp."""
    from datetime import datetime
    filename = filename_base + datetime.now().strftime('.%Y%m%d.%H%M%S') + \
        file_extension
    return filename

def getPaths(source_log_path, source_stat_path, source_data_path, \
                 source_email_path, source_viz_path):
    """Create dictionary of paths set in params.py."""
    path_dict = dict(zip(['log', 'stat', 'bib.txt', 'email', 'viz.txt', \
                              'viz.sql'], \
                             [source_log_path, source_stat_path, \
                                  source_data_path, source_email_path, \
                                  source_viz_path, source_viz_path]))
    return path_dict

def doFileRotation(source_rotation_data, path_dict, source_archive_path, \
                       source_email_server, source_email_sender, \
                       source_email_recipients):
    """Archive and delete files according to settings in params.py"""
    import os
    for action in source_rotation_data:
        for file_ext in source_rotation_data[action]:
            try:
                settings = dict(zip(['value', 'unit', 'output'], \
                                        source_rotation_data[action][file_ext]))
            except KeyError:
                continue
            files = [f for f in os.listdir(path_dict[file_ext]) \
                         if f.endswith(file_ext)]
            for f in files:
                file_action = False
                if settings['value'] is True:
                    file_action = newMonthTest(f)
                elif settings['value'].isdigit():
                    file_age = getFileAge(path_dict[file_ext] + f, \
                                              settings['unit'])
                    if file_age > int(settings['value']):
                        file_action = True
                if file_action is True:
                    if settings['output'] is True:
                        sendRotationOutput(action, path_dict[file_ext] + f, \
                                               source_email_server, \
                                               source_email_sender, \
                                               source_email_recipients)
                    if action == 'archive':
                        os.rename(path_dict[file_ext] + f, \
                                      source_archive_path + f)
                    elif action == 'delete':
                        os.remove(path_dict[file_ext] + f)

def sendRotationOutput(action, action_file, server, sender, recipients):
    """Send email notification when rotation action taken."""
    from datetime import date
    subject = 'Solr audit file %s attached' % action_file
    report = 'Date: %s\nFile: %s\nAction: %s\n\nFile is attached.' \
        % (date.today(), action_file, action)
    writeEmail(server, sender, recipients, subject, report, \
                   attach=[action_file])
                    
def getFileAge(filename, unit):
    """Get age of file in months or days."""
    from datetime import date, timedelta
    import os
    now = date.today()
    file_mod_time = date.fromtimestamp(os.stat(filename).st_mtime)
    if unit == 'day':
        d = now - file_mod_time
        file_age = d.days
    elif unit == 'month':
        if now.month < file_mod_time.month:
            file_age = (now.month + 12) - file_mod_time.month
        else:
            file_age = now.month - file_mod_time.month
    return file_age

def newMonthTest(filename):
    from datetime import date
    now = '{0:%Y%m}'.format(date.today())
    if now not in filename:
        return True
    else:
        return False

def confirmDir(path):
    """Confirm the existence of a directory."""
    import os
    if not os.access(path, os.F_OK):
        os.mkdir(path)

def processArgs(args):
    """Process command line options."""
    import getopt, sys
    try:
        optlist, args = getopt.getopt(args, 'd:r:s:v:', ['date=', 'resend=',
                                                         'source=', 'viz='])
    except getopt.GetoptError as err:
        print err
        usage()
        sys.exit()
    arg_dict = {'date': None, 'resend': None, 'source': None, 'viz': None}
    for o, a in optlist:
        if o in ('-d', '--date'):
            arg_dict['date'] = a
        elif o in ('-r', '--resend'):
            arg_dict['resend'] = a
        if o in ('-s', '--source'):
            arg_dict['source'] = a
        if o in ('-v', '--viz'):
            arg_dict['viz'] = a
    return arg_dict

def usage():
    """Info message about command-line options."""
    print 'Options: use -d or --date=YYYYMMDD to set alternate audit date; ' + \
        'use -r or --resend=YYYYMMDD to resend email notifications from ' + \
        'indicated date; use -s or --source=source1 or source2 to limit ' + \
        'audit or optional action to one datasource; use -v or -viz=off or ' + \
        'on to toggle output to data visualization service. All options ' + \
        'may be combined, except -r with -d or -v.'

def setDate(date):
    """Format date from command line argument."""
    import sys
    if len(date) != 8:
        print 'Please enter date in form YYYYMMDD.'
        sys.exit()
    alternate_date = date[:4] + '-' + date[4:6] + '-' + date[6:]
    return alternate_date

def resendEmail(source_name, filebase, server, date, email_path, archive_path):
    """Resend email notifications for date from command line argument."""
    import smtplib, email, sys
    path_list = [email_path, archive_path]
    email_files = matchFile(filebase, 'email', path_list, date)
    if not email_files:
        print 'No emails found from %s for %s.' % (source_name, date)
        return 0
    smtp = smtplib.SMTP(server)
    for f in email_files:
        msg = email.message_from_file(file(f))
        sender = msg['from']
        recipients = msg['to']
        smtp.sendmail(sender, recipients, msg.as_string())
    smtp.close()
    return 1

def matchFile(filebase, extension, path_list, date):
    """Identify matching files in designated paths from filename base, 
    extension, and audit date."""
    import os
    match_files = []
    for path in path_list:
        try:
            for f in os.listdir(path):
                parts = f.split('.')
                if parts[0] == filebase and parts[1] == date and \
                        parts[-1] == extension:
                    match_files.append(path + f)
        except OSError:
            pass
    return match_files

def writeToDatabase(db_data, viz_sql_data):
    """Write stats to database for use by visualization service."""
    import MySQLdb as sql
    db = sql.connect(db_data['host'], db_data['user'], db_data['passwd'], \
                         db=db_data['db'])
    c = db.cursor()
    query_dict = {}
    with db:
        for filename, values in viz_sql_data.iteritems():
            db_write = "REPLACE INTO Audit_Stats(Audit_Date, TimeStamp, Resource, Action, Extract, Error, `Load`, Filename) VALUES('%(audit_date)s', '%(timestamp)s', '%(resource)s', '%(action)s', %(extract)d, %(error)d, %(load)d, '%(filename)s');" % values
            c.execute(db_write)
            if values['action'] in query_dict:
                query_dict[values['action']].append(db_write)
            else:
                query_dict[values['action']] = [db_write]
    return query_dict

def writeSQLToFile(viz_path, filename_base, add_alt, extension, query_dict):
    """Write SQL queries to file by action."""
    for action, queries in query_dict.iteritems():
        filename = getFileNameTimestamp(filename_base + '_' + action + add_alt,\
                                            extension)
        with open(viz_path + filename, 'w') as fh:
            for q in queries:
                fh.write(q + '\n\n\n')
