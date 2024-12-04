import logging

a = 1
a += a
log_file_path = r"C:\Users\Shane\Desktop\Apllo\task_log.log"
logging.basicConfig(filename=log_file_path, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Directly log the integer with formatting
logging.info("The value of a is %d", a)
