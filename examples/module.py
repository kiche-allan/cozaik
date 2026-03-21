from ticktalkpython.SQ import SQify

@SQify
def reset_device(trigger):
    import time
    print('resetting device...')
    time.sleep(3)
    print('done!')
    return trigger

@SQify
def const_val():
    return 0
