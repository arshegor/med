from multiprocessing import Process
from sys import argv

import service
import http_post

if __name__ == '__main__':
    procService = Process(target=service.main, args=("/data/",))
    procHttpPost = Process(target=http_post.main)
    procService.start()
    procHttpPost.start()


    run = True
    while run:
        if not procService.is_alive():
            procService.kill()
            run = False
        if not procHttpPost.is_alive():
            procHttpPost.kill()
            run = False
