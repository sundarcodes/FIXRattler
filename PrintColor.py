


class bcolors:
    HEADER = '\033[95m'
    OKGREEN = '\033[92m'
    OKBLUE = '\033[94m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'

def disable():
    HEADER = ''
    OKGREEN = ''
    OKBLUE = ''
    WARNING = ''
    FAIL = ''
    ENDC = ''
    BOLD = ''

def print_success(msg):
    print(bcolors.OKBLUE+msg+bcolors.ENDC);

def print_info(msg):
    print(bcolors.OKBLUE+msg+bcolors.ENDC);

def print_warn(msg):
    print(bcolors.WARNING+msg+bcolors.ENDC);

def print_err(msg):
    print(bcolors.FAIL+msg+bcolors.ENDC);

def print_bold(msg):
    print(bcolors.BOLD+msg+bcolors.ENDC);

def print_header(msg):
    print(bcolors.HEADER+msg+bcolors.ENDC);
