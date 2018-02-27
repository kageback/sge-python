

def enforce_trailing_backslash(path):
    if len(path) > 0 and path[-1] != '/':
        path += '/'
    return path


def select_shell(cmd_list, host='localhost'):
    if host == 'localhost':
        return cmd_list
    else:
        cmd = 'ssh ' + host + ' bash --login -c "' + ' '.join(cmd_list) + '"'
        return cmd.split()



def slugify(value):
    import re
    value = re.sub('[^\w\s-]', '', value).strip()
    value = re.sub('[-\s]+', '-', value)
    return value
