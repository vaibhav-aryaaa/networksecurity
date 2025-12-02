import sys
from networksecurity.logging import logger

class NetworkSecurityException(Exception):
    def error_message_detail(self,error,error_detail:sys):
        self.error_message=error_message
        _,_,exc_tb=error_detail.exc_info()
        
        self.lineno=exc_tb.tb_lineno
        self.file_name=exc_tb.tb_frame.f_code.co_filename
    
    def __str__(self):
        return "Error occured in python script name {} line number{} error message{}".format(
        self.file_name, self.lineno, str(self.error_message))
   
if __name__=='__main__':
    try:
        logger.logging.info("ENTER THE TRY BLOCK")
        a=1/0
        print("this will not be printed",a)
    except Exception as e:
        raise NetworkSecurityException(e,sys)    

