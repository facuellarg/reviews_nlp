from bs4 import BeautifulSoup
from request_handler import req_get
from requests import get
import os.path
import json


def normalize_text(s):
    """ Quita acentos, espacios y capitalizción de cadenas."""
    replacements = (("á", "a"), ("é", "e"), ("í", "i"), ("ó", "o"),
                    ("ú", "u"), ("ñ","n"), ("  "," "), (" ","-"))
    s = s.strip()
    for a, b in replacements:
        s = s.replace(a, b).replace(a.upper(), b.upper())
        s = s.lower()
    return s


def compose_prof_url(departamento, docente):
    """ Retorna una cadena 

    Dado un profesor y su departamento retorna la url del profesor en
    <losestudiantes.co>.
    """
    base_url = "https://losestudiantes.co/universidad-nacional/"
    url = "{}{}/profesores/{}".format(base_url, departamento,docente)
    return(url)


def get_urls_artes(url, docentes):
    """ Retorna un diccionario

    las llaves son las escuelas de la facultad,
    los valores son listas de proferores de dicha escuela.
    """
    #html_artes = req_get(url).text
    html_artes = req_get(url)
    arbol = BeautifulSoup(html_artes, "html.parser")
    escuelas = arbol.find_all("option")

    for escuela in escuelas:
        docente_dep = []
        new_url = url + "?escuela=" + escuela["value"]
        #new_html = req_get(new_url).text
        new_html = req_get(new_url)
        arbol = BeautifulSoup(new_html, "html.parser")
        for i in arbol.find_all("span", {"class":"name"}):
            docente_dep.append(normalize_text(i.text))
        docentes[normalize_text(arbol.find("option",selected=True).text)] = docente_dep

    docentes["artes-plasticas-y-visuales"] = docentes.pop("artes-plasticas")
    return docentes


def get_urls_fce(url):
    """ Retorna una lista de profesores de la facultad de ciencias economicas."""
    #html =  req_get(url).text
    html =  req_get(url)
    arbol = BeautifulSoup(html, "html.parser")
    docente_dep = []
    for professor in arbol.find_all("h4", {"style":"font-size: 16px"}):
        docente_dep.append(normalize_text(professor.text))
    return docente_dep


def get_urls_unal():
    """ Retorna una lista con las urls de los profesores de la UNal.

    Escribe en un archivo de texto con la lista de profesores de la UNal.
    """
    url_artes = "http://www.facartes.unal.edu.co/fa/docentes/index.php"
    url_admin = "http://fce.unal.edu.co/docentesfce/index.php?escuela=1"
    url_econ = "http://fce.unal.edu.co/docentesfce/index.php?escuela=2"

    docentes_unal = get_urls_artes(url_artes, {})
    docentes_unal["administracion-y-contaduria-publica"] = get_urls_fce(url_admin)
    docentes_unal["economia"] = get_urls_fce(url_econ)
    
    with open("./data/professors.json", "w") as write_professors_file:
        json.dump(docentes_unal, write_professors_file)

    urls_unal = []
    for k, v in docentes_unal.items():
        for profesor in v:
            urls_unal.append(compose_prof_url(k, profesor)) # USING URLS LIST
    return urls_unal


def get_urls_uniandes():
    """ Retorna una lista de urls

    Lee un archivo de texto, con info. de los profesores de Uniandes y 
    compone su url correspondiente para <losestudiantes.co>
    """
    urls = []
    with open("./data/url_andes.txt","r") as url_andes:
        arbol = BeautifulSoup(url_andes.read(), "html.parser")
        for a in arbol.find_all("a", {"class":"jsx-633353764"}):
            urls.append("https://losestudiantes.co/" + a["href"])
    #print(len(urls))
    return urls


def get_train_data(urls):
    """ Recibe una lista de urls

    Busca si ya existe un archivo con los textos de reseñas,
    si no existe recorre la lista de urls para extraer los textos
    """
    train_data = []
    if os.path.isfile("./data/reviews.json"):
        with open("./data/reviews.json", "r") as reviews_file:
            train_data = json.load(reviews_file) # Read data from file
    else:
        for url in urls:
            html_review = get(url).text
            arbol = BeautifulSoup(html_review, "html.parser")
            containers = arbol.find_all("li", {"class":"jsx-571610088 post "})
            for container in containers:
                review = container.find("div", {"class":"jsx-571610088 lineBreak"})
                nota = container.find("span", {"class":"jsx-571610088 numeroStats"})
                if nota is None or review is None:
                    continue
                train_data.append((review.text, float(nota.text), url))

        with open("./data/reviews.json", "w") as reviews_file:
            json.dump(train_data, reviews_file)
    return train_data


def main():
    urls_unal = get_urls_unal()
    urls_andes = get_urls_uniandes()
    urls_all = urls_unal + urls_andes
    train_data = get_train_data(urls_all)


if __name__ == "__main__":
    main()
