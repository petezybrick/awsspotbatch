'''
Created on Mar 19, 2015

@author: pete.zybrick
'''
import sys
import traceback
import subprocess
import os

def main():     
    if( len(sys.argv) < 3 ):
        print 'Invalid format, execution cancelled'
        print 'Correct format: python awsspotbatch.spotclientinit <cmdfile.txt> <loggerConfigFile.conf>'
        sys.exit(8)
    
    import logging.config
    logging.config.fileConfig( sys.argv[2], disable_existing_loggers=False)
    logger = logging.getLogger(__name__)
    
    try:
        logger.info( 'Starting, cmd_file=' + sys.argv[1] + ', loggerConfigFile=' + sys.argv[2] )
        with open(sys.argv[01]) as f:
            cmds = f.readlines()
            
        for cmd in cmds:
            cmd = cmd.strip()
            if len( cmd ) == 0: continue    # blank line, allow for readability            
            if '#' == cmd[0:1]:
                logger.info("Comment: " + cmd.strip() ) 
                continue        # skip comments
            cmd_name_args = cmd.split( ' ' )
            logger.info('Cmd: ' + str(cmd_name_args))
            if cmd_name_args[0].lower() != 'cd':
                child_process = subprocess.Popen( cmd_name_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE )
                std_out, std_err = child_process.communicate( )
                returncode = child_process.returncode
            # CD not supported by emulated terminal in Python
            else: 
                if cmd_name_args[1][0:1] != '~': cd_path = os.path.abspath(cmd_name_args[1])
                else: cd_path = os.path.abspath(os.path.expanduser( cmd_name_args[1] )) 
                os.chdir(cd_path)    
                std_out = ''
                std_err = ''
                returncode = 0

            logger.info('returncode=' + str(returncode) + ', std_out=' + std_out + ', std_err=' + std_err )
            if returncode > 0: sys.exit( returncode )

        
        logger.info( 'Completed Successfully' )

    except StandardError as e:
        logger.error( e )
        logger.error( traceback.format_exc() )
        sys.exit(8)

      
if __name__ == "__main__":
    main()