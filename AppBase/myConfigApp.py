
import os
import yaml
import logging
from logging import config
from dotenv import load_dotenv

class ConfigApp:

    configDictionary={}

    def __int__(self):
        load_dotenv()
        self.log_cfg=os.environ.get("LOG_FILE_CONFIG")
        if self.log_cfg!=None:
            config.fileConfig(self.log_cfg)

        self.cfg_file = os.environ.get("YARN_FILE_CONFIG")
        if self.cfg_file!=None:
            self.configDictionary = self.read_yaml(self.cfg_file)

    def __init__(self,argv_sys):

        self.log_cfg=self.argv_log_cfg(argv_sys)

        config.fileConfig(self.log_cfg)

        self.cfg_file=self.argv_A_cfg(argv_sys)

        self.configDictionary=self.read_yaml(self.cfg_file)
        print(self.configDictionary)
        self.set_environ()


    def GetLogger(self):
        return logging.getLogger("root")


    def GetProperties(self):
        return self.configApp


    def argv_log_cfg(self,sys_argv):
            print("The sys.argv list is:", sys_argv)
            sys_argv_length = len(sys_argv)
            number_of_arguments = sys_argv_length - 1
            print("Total command line arguments are:", number_of_arguments)

            counter = -1
            log_pos = -1
            cfg_pos = -1
            for pos in sys_argv:
                counter = counter + 1
                print(f"index{counter}  value {pos} ")
                if (pos == "-Log"):
                    log_pos = counter
                if (log_pos > -1 and counter == log_pos + 1):
                    return pos
            return None

    def argv_A_cfg(self,sys_argv):
            print("The sys.argv list is:", sys_argv)
            sys_argv_length = len(sys_argv)
            number_of_arguments = sys_argv_length - 1
            print("Total command line arguments are:", number_of_arguments)

            counter = -1

            cfg_pos = -1
            for pos in sys_argv:
                counter = counter + 1
                print(f"index{counter}  value {pos} ")
                if (pos == "-F_cfg"):
                    cfg_pos = counter
                if (cfg_pos > -1 and counter == cfg_pos + 1):
                    print(pos)
                    return pos
            return None

    def read_yaml(self,file_path):
            with open(file_path, "r") as f:
                return yaml.safe_load(f)

    def set_environ(self):
        if ("SET_ENVIRONMENT" in self.configDictionary):
            print("OK hay SET_ENVIRONMENT")
            print(self.configDictionary["SET_ENVIRONMENT"])
            dic_env=self.configDictionary["SET_ENVIRONMENT"]
            for pkey in dic_env :
                os.environ[pkey]=dic_env[pkey]
        else:
            print("KO no hay SET_ENVIRONMENT")
