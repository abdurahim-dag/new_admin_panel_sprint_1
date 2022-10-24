import re


def file_rename(name, sufix):
    m = re.search('(.+)(\..+$)', name)
    new_name = m[1] + '_' + sufix + m[2]
    return new_name
