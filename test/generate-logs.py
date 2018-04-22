from random import randint
import time

"""
This is use to create a file one by one in each 5 seconds interval. 
These files will store content dynamically from 'apache-log.txt' using below code
"""


def main():
    a = 1
    with open('apache-log.txt', 'r') as file:   
        lines = file.readlines()
        while a <= 1:
            totalline = len(lines)
            # = randint(0, totalline - 10)
            with open('output/log{}.txt'.format(a), 'w') as writefile:
                writefile.write(' '.join(line for line in lines[0:totalline]))
            print('creating file log{}.txt'.format(a))
            a += 1
            #time.sleep(5)


if __name__ == '__main__':
    main()