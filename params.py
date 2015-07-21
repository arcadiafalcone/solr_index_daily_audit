#!/usr/bin/python

default = {
# Sets default values to facilitate updating all sources at once. If value is
# None, set parameter at source level (except in the case of alternate_date,
# whose default is None). The default value will apply only if the source 
# parameter is set to default['key']; any other value in the source parameter
# will override the default.
        # Name of source for use in output
        'name': None,
        # Date to audit if not today - enter as 'YYYY-MM-DD' (with quotes)
        # Default is None (no quotes)
        'alternate_date': None,
        # Location of processed extract files from source
        'input_path': '/path/to/files',
        # Regex patterns for identifying add, suppress, delete files
        'input_filenames': {
            'add': None,
            'suppress': None,
            'delete': None},
        # Skip MFHDs if present in .mrc file (value is True or False)
        'skip_MFHDs': None,
        # Set pattern to recognize bibliographic IDs in text files
        'bib_pattern': None,
        # URL to query Solr index
        # 'rows' parameter must be greater than total number of records in 
        # index from all sources
        # Derived from information at:
        # https://nowontap.wordpress.com/2014/04/04/solr-exporting-an-index-to-an-external-file/
        'solr_url': 'http://localhost:8888/solr/collection/select?q=&start=0&rows=12000000&fl=id%2C+timestamp&wt=csv&indent=true',
        # Path for log output
        'log_path': 'logs/',
        # Filename for log file (output will append)
        # Extension must be .log
        # Must include YYYYMM
        'log_file': 'solr_audit_file_YYYYMM.log',
        # Set whether to write to log if no new files found
        'log_if_none': False,
        # Path for stats output
        'stat_path': 'logs/',
        # Filename for stats file (output will append)
        # Extension must be .stat
        # Must include YYYYMM
        'stat_file': 'statfile_YYYYMM.stat',
        # Path for data file output (files contain bib ids processed)
        'data_path': 'data/',
        # Filename patterns for data files
        # Full filename is: first value term + .YYYYMMDD.hhmmss.txt
        # Currently set in individual datasource params
        # Second term sets whether file is sent with email (True or False)
        'data_filenames': {
            'solr_added': (None, False),
            'solr_deleted': (None, False),
            'solr_not_added': (None, False),
            'solr_not_deleted': (None, False)},
        # Path for archived files
        'archive_path': 'archive/',
        # Path for email file output (backup of email notification)
        'email_path': 'logs/',
        # Filename for email backup file
        # Full filename is this value + .YYYYMMDD.hhmmss.txt
        # Set in individual datasource parameters
        'email_filename': None,
        # Recipients for email notifications
        'email_recipients': [
            'recipient1@example.com',
            'recipient2@example.com'],
        'email_server': 'mail',
        'email_sender': '"Solr Audit" <noreply@example.com>',
        # Stats output for data visualization dashboard
        # Set whether data is exported to visualization service: 'on' or 'off'
        'viz_output': 'on',
        # Information to access visualization stats database
        'viz_output_db': {
            'host': 'hostname',
            'user': 'username',
            'passwd': 'password',
            'db': 'database'},
        # Path for visualization stat file
        'viz_output_path': 'viz/',
        # Filename for visualization file backup of SQL command
        # Full filename is this value + .YYYYMMDD.hhmmss.viz.sql
        # Set in individual datasource parameters
        'viz_output_filename': None,
        # Time after which to take file rotation action (value in given units)
        # Key matches file extension
        # Recognized units are 'day' and 'month'
        # Third term = True sends file as email attachment when action is taken
        # For log and stat: first term is whether to archive at the first time
        #   the script runs in a calendar month, third term is whether to send 
        #   files when archived
        'rotation_data': {
            'archive': {
                'log': (True, None, True),
                'stat': (True, None, True),
                'bib.txt': ('7', 'day', False),
                'email': ('1', 'day', False),
                'viz.sql': ('7', 'day', 'False')},
            'delete': {
                'log': ('6', 'month', False),
                'stat': ('6', 'month', False),
                'bib.txt': ('1', 'month', False),
                'email': ('7', 'day', False),
                'viz.sql': ('14', 'day', False)}
            },
}

config = [
### SOURCE 1
# See default parameters above for explanatory comments.
    {
        'name': 'source1_name',
        'alternate_date': default['alternate_date'],
        'input_path': default['input_path'],
        'input_filenames': {
            'add': r'source1_MMDDYYYY\.\d*\.mrc',
            'suppress': r'source1\.suppr\.YYYYMMDD\.del\.txt',
            'delete': r'source1\.deleted-bibids\.YYYYMMDD\.del\.txt'},
        'skip_MFHDs': True,
        'bib_pattern': '^\d+',
        'solr_url': default['solr_url'],
        'log_path': default['log_path'],
        'log_file': default['log_file'],
        'log_if_none': default['log_if_none'],
        'stat_path': default['stat_path'],
        'stat_file': default['stat_file'],
        'data_path': default['data_path'],
        'data_filenames': {
            'solr_added': ('source1_add_success', True),
            'solr_deleted': ('source1_del_success', True),
            'solr_not_added': ('source1_add_error', True),
            'solr_not_deleted': ('source1_del_error', True)},
        'archive_path': default['archive_path'],
        'email_path': default['email_path'],
        'email_filename': 'source1',
        'email_recipients': default['email_recipients'],
        'email_server': default['email_server'],
        'email_sender': default['email_sender'],
        'viz_output': default['viz_output'],
        'viz_output_db': {
            'host': default['viz_output_db']['host'],
            'user': default['viz_output_db']['user'],
            'passwd': default['viz_output_db']['passwd'],
            'db': default['viz_output_db']['db']},
        'viz_output_path': default['viz_output_path'],
        'viz_output_filename': 'source1',
        'rotation_data': {
            'archive': {
                'log': default['rotation_data']['archive']['log'],
                'stat': default['rotation_data']['archive']['stat'],
                'bib.txt': default['rotation_data']['archive']['bib.txt'],
                'email': default['rotation_data']['archive']['email'],
                'viz.sql': default['rotation_data']['archive']['viz.sql']},
            'delete': {
                'log': default['rotation_data']['delete']['log'],
                'stat': default['rotation_data']['delete']['stat'],
                'bib.txt': default['rotation_data']['delete']['bib.txt'],
                'email': default['rotation_data']['delete']['email'],
                'viz.sql': default['rotation_data']['delete']['viz.sql']}
            }
        },

### SOURCE 2
# See default parameters above for explanatory comments
    {
        'name': 'source2',
        'alternate_date': default['alternate_date'],
        'input_path': default['input_path'],
        'input_filenames': {
            'add': r'source2-updates\.YYYYMMDD\.flip\.mrc',
            'suppress': r'source2-suppress-del-YYYY-MM-DD-\d*\.txt',
            'delete': r'source2-deletes-YYYY-MM-DD-\d*\.txt'},
        'bib_pattern': '^b\d+',
        'skip_MFHDs': False,
        'solr_url': default['solr_url'],
        'log_path': default['log_path'],
        'log_file': default['log_file'],
        'log_if_none': default['log_if_none'],
        'stat_path': default['stat_path'],
        'stat_file': default['stat_file'],
        'data_path': default['data_path'],
        'data_filenames': {
            'solr_added': ('source2_add_success', True),
            'solr_deleted': ('source2_del_success', True),
            'solr_not_added': ('source2_add_error', True),
            'solr_not_deleted': ('source2_del_error', True)},
        'archive_path': default['archive_path'],
        'email_path': default['email_path'],
        'email_filename': 'source2',
        'email_recipients': default['email_recipients'],
        'email_server': default['email_server'],
        'email_sender': default['email_sender'],
        'viz_output': default['viz_output'],
        'viz_output_path': default['viz_output_path'],
        'viz_output_db': {
            'host': default['viz_output_db']['host'],
            'user': default['viz_output_db']['user'],
            'passwd': default['viz_output_db']['passwd'],
            'db': default['viz_output_db']['db']},
        'viz_output_filename': 'source2',
        'rotation_data': {
            'archive': {
                'log': default['rotation_data']['archive']['log'],
                'stat': default['rotation_data']['archive']['stat'],
                'bib.txt': default['rotation_data']['archive']['bib.txt'],
                'email': default['rotation_data']['archive']['email'],
                'viz.sql': default['rotation_data']['archive']['viz.sql']},
            'delete': {
                'log': default['rotation_data']['delete']['log'],
                'stat': default['rotation_data']['delete']['stat'],
                'bib.txt': default['rotation_data']['delete']['bib.txt'],
                'email': default['rotation_data']['delete']['email'],
                'viz.sql': default['rotation_data']['delete']['viz.sql']}
            }
        }
]
