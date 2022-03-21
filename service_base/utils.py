import os
import time
from service_base import settings

class Dir:
    def __init__(self, goto, create_if_unexist=True, backto=settings.BASE_DIR):
        """
        Receive those parameters to change dir back and forth
        :param goto:
        :param backto:
        """

        self.goto = goto
        self.backto = backto


    def __enter__(self):
        try:
            os.chdir(self.goto)
        except:
            os.mkdir(self.goto)
            os.chdir(self.goto)

    def __exit__(self, exc_type, exc_val, exc_tb):
        os.chdir(self.backto)
        # THIS MUST BE THE LAST LINE of __exit__
        del self


def exec_and_wait(c, wait=2):
    os.system(c)
    time.sleep(2)