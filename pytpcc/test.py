from multiprocessing import Pool
import time


def fun_01(i):
    print('start_time:', time.ctime())
    time.sleep(2)
    return i + 100


def fun_02(arg):
    print('end_time:', arg, time.ctime())


if __name__ == '__main__':
    pool = Pool(3)
    for i in range(4):
        pool.apply_async(func=fun_01, args=(i,), callback=fun_02)  # fun_02的入参为fun_01的返回值
        # pool.apply_async(func=fun_01, args=(i,))
    pool.close()
    pool.join()
    print('done')