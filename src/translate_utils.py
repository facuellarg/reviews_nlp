from googletrans import Translator
import threading
import json
import os
import math
import pandas as pd
import time
reviews_trasnlated_file="./data/reviews_translated/reviews_translated.json"

mutex = threading.Lock()
def get_translated_reviews(df, num_threads=2,path_file=reviews_trasnlated_file,start=0):
    if os.path.isfile(path_file):
        with open(path_file,"r") as f:
            r_t = json.load(f)
            return pd.DataFrame(r_t.items())
    threads = list()
    reviews = df[0].to_numpy()
    translator = Translator()
    t_t=[0]*len(reviews)
    tile = math.ceil(len(reviews)/num_threads)
    def translate(text_to_translate ,part): 
        for index in range(tile*part, tile*(part+1)):
            tries=0
            if index >= len(text_to_translate):
                break
            #print("hilo: %s index: %s"%(part,index))
            text=text_to_translate[index]
            while(text == text_to_translate[index]):
                if tries > 10 :
                    print("no se pudo traducir", text)
                    mutex.acquire()
                    time.sleep(1)
                    mutex.release()
                    break
                try:
                    text=translator.translate(text_to_translate[index],src='es', dest='en').text                    
                except Exception as E:
                    print(text)
                    print(E)
                    pass
                tries+=1
            t_t[index]=(text, index+start)
    print("traduciendo los datos, esto puede tardar algunas horas")
    for i in range(num_threads):
        t = threading.Thread(target=translate, args=(reviews,i))
        threads.append(t)
        t.start()
    
    for i in range(num_threads):
        threads[i].join()
    reviews_translated = {t_t[i][0]:(df[1].iloc[i],t_t[i][1]) for i in range(len(t_t))}
    with open(path_file, "+w") as f:
        json.dump(reviews_translated,f,indent=2, ensure_ascii=False)
    return pd.DataFrame(reviews_translated.items())


def translate_sentence(sentence):
    translator = Translator()
    return translator.translate(sentence,src='es', dest='en').text
