import os
from time import sleep

from market.api.__main__ import main

if __name__ == '__main__':
    sleep(5)
    os.system('python market/db/__main__.py upgrade head')
    main()
