from flask import Flask , jsonify
import requests
from bs4 import BeautifulSoup
import json
import re
port = 5003
app = Flask(__name__)

@app.route('/', methods=['GET'])
def index():
    return "¡Bienvenido a mi aplicación Flask!"

def escribir_archivo(url,data):
    with open('{}.txt'.format(url), 'w') as archivo:
        archivo.write(data)

@app.route('/scrapi/<url>')
def scraping_data(url):
    
    
    index = {}
    data=""
    try:
        response = requests.get("https://"+url+"/") # Hacer una solicitud GET al sitio web
        soup = BeautifulSoup(response.text, 'html.parser') # Analizar el contenido HTML de la página
        # Extraer todas las palabras clave de la página web
        words = re.findall('\w+', soup.text)
        for word in words:
                data += word + "\n"
        
        escribir_archivo(url,data)
        return jsonify({'status': "ok" })
    except:
        print("error...")
        return   jsonify({'status': "Algun error..." })


if __name__ == '__main__':
    app.run(debug=True, port=port)
