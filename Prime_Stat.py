# -*- coding: utf-8 -*-
"""
Created on Tue May 14 12:32:55 2019

@author: mmelkowski
"""

import pandas as pds
import time
import threading
import queue


class stat_primes:
    def __init__(self, fname="primes.txt", nb_thread_process=3):

        # NB of Thread for process
        self.nb_thread_process = nb_thread_process

        # PREPARATION DICO
        self.final_dict = {'1': {'1': 0, '3': 0, '7': 0, '9': 0},
                           '3': {'1': 0, '3': 0, '7': 0, '9': 0},
                           '7': {'1': 0, '3': 0, '7': 0, '9': 0},
                           '9': {'1': 0, '3': 0, '7': 0, '9': 0}
                           }
        self.dico_keys = list(self.final_dict.keys())  # ["1","3","7","9"]

        # INIT
        self.block_list = queue.Queue()

        self.f_line_to_do = True

        self.prepare_file(fname)
        print("Start_main")
        self.main()
        print("Straight out of main")
        self.close_file()

    def prepare_file(self, fname):
        self.f = open(fname, "r")
        for _ in range(3):
            self.f.readline()
        self.first_elt = self.f.readline()

    def close_file(self):
        self.f.close()

    def count_block_into_dico(self, dico_count, block):
        i = 1
        val = block[i-1][-2]
        while i < len(block):
            follow = block[i][-2]
            dico_count[val][follow] += 1
            val = follow
            i += 1
        return dico_count

    def add_count(self, dico_count):
        for index in self.dico_keys:
            for col in self.dico_keys:
                self.final_dict[index][col] += dico_count[index][col]

    def create_block(self):
        while self.f_line_to_do:
            if self.block_list.qsize() < 10:
                block = self.f.readlines((1024**2)*2)
                if block == []:
                    self.f_line_to_do = False
                    print("end of file")
                    break
                self.block_list.put([self.first_elt] + block)
                self.first_elt = block[-1]

    def threader(self):
        dico_count = {'1': {'1': 0, '3': 0, '7': 0, '9': 0},
                      '3': {'1': 0, '3': 0, '7': 0, '9': 0},
                      '7': {'1': 0, '3': 0, '7': 0, '9': 0},
                      '9': {'1': 0, '3': 0, '7': 0, '9': 0}
                      }

        while (self.f_line_to_do or not self.block_list.empty()):
            # Retreive the block of prime number to process
            if not self.block_list.empty():
                try:
                    block = self.block_list.get(False)
                except queue.Empty:
                    block = []
            else:
                block = []

            # If block is not empty -> count_block_into_dico
            if block:
                dico_count = self.count_block_into_dico(dico_count, block)

        # After the file is complet
        with self.add_lock:      # ADD_LOCK
            self.add_count(dico_count)

    def main(self):
        # Thread Lock for adding result in final_dict
        self.add_lock = threading.Lock()

        list_thread = []

        # THREAD FOR READING:
        t = threading.Thread(target=self.create_block)
        #  classifying as a daemon, so they will die when the main dies
        t.daemon = True
        #  begins, must come after daemon definition
        t.start()
        list_thread.append(t)

        # THREAD for processing block:
        for x in range(self.nb_thread_process):
            t = threading.Thread(target=self.threader)
            t.daemon = True
            t.start()
            list_thread.append(t)

        for t in list_thread:
            t.join()

    def primes_into_df(self):
        self.prime_dataframe = pds.DataFrame(self.final_dict).T
        self.prime_count = self.prime_dataframe

    def mean_primes(self):
        self.prime_dataframe = self.prime_dataframe.T
        for col in self.prime_dataframe.columns:
            self.prime_dataframe[col] = round((self.prime_dataframe[col] /
                                               self.prime_dataframe[col].sum()
                                               )*100, 2)
        self.prime_dataframe = self.prime_dataframe.T


"""
# solution petit fichier
{'1': {'1': 42853, '3': 77475, '7': 79453, '9': 50153},
 '3': {'1': 58255, '3': 39668, '7': 72827, '9': 79358},
 '7': {'1': 64230, '3': 68595, '7': 39603, '9': 77586},
 '9': {'1': 84596, '3': 64371, '7': 58130, '9': 42843}}
"""


if __name__ == "__main__":
    snakeviz_to_do = False
    if snakeviz_to_do:
        start = time.time()
        import cProfile, pstats, io
        from pstats import SortKey
        pr = cProfile.Profile()
        pr.enable()

        # ... do something ...
        #stat = stat_primes(fname="C:/Users/mmelkowski/Downloads/bigListe.txt", nb_thread_process = 4)
        stat = stat_primes(fname="primes.txt", nb_thread_process=4)

        pr.disable()
        s = io.StringIO()
        sortby = SortKey.CUMULATIVE
        ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
        ps.print_stats()
        # print(s.getvalue())
        pr.dump_stats("profile_snakeviz_file.prof")

        # Follow
        stat.close_file()
        stat.primes_into_df()
        stat.mean_primes()
        end = time.time() - start
        print(end)

    else:
        start = time.time()
        #stat = stat_primes(fname="C:/Users/mmelkowski/Downloads/bigListe.txt", nb_thread_process = 4)
        stat = stat_primes("primes.txt", nb_thread_process=4)
        stat.close_file()
        stat.primes_into_df()
        stat.mean_primes()
        end = time.time() - start
        print(end)

    graph = False
    if graph:
        import numpy as np
        import matplotlib.pyplot as plt
        with plt.xkcd():
            T = np.array([1, 2, 3, 4, 5])
            S = np.array([186, 156, 108.7, 107.9, 107.28])

            plt.scatter(T, S, c="firebrick", zorder=1)
            plt.plot(T, S, zorder=0)
            plt.show()
