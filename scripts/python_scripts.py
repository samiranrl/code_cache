import os, psutil; print(psutil.Process(os.getpid()).memory_info().rss / 1024 ** 2) # gets you the resident set size
