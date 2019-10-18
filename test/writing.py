import time


def writing(msg, n):
    for i in range(n):
        print('{} {}'.format(str(i), msg))
        time.sleep(1)
    return 'That was some heavy counting. All the way to {}!!'.format(n), n


if __name__ == '__main__':
    writing()
