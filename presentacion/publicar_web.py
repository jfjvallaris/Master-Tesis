# -*- coding: utf-8 -*-
"""Arma docs/ para GitHub Pages a partir del render de Presentacion.qmd.

Las notas del expositor se conservan: son comentarios de Claude sobre la
lectura del trabajo final, escritos para que quien recorra el deck online
tenga la ampliacion de cada pantalla.

Uso:
    quarto render Presentacion.qmd
    python publicar_web.py
"""
import io, re, shutil
from pathlib import Path

AQUI = Path(__file__).resolve().parent
DOCS = AQUI.parent / 'docs'
FUENTE = AQUI / 'Presentacion.html'

if not FUENTE.exists():
    raise SystemExit('Falta Presentacion.html: correr primero  quarto render Presentacion.qmd')

html = io.open(FUENTE, encoding='utf-8').read()

if DOCS.exists():
    shutil.rmtree(DOCS)
DOCS.mkdir()

# el deck es la portada del sitio
io.open(DOCS / 'index.html', 'w', encoding='utf-8', newline='\n').write(html)
# Pages no debe pasar la salida por Jekyll
io.open(DOCS / '.nojekyll', 'w', encoding='utf-8').write('')

shutil.copytree(AQUI / 'Presentacion_files', DOCS / 'Presentacion_files',
                ignore=shutil.ignore_patterns('desktop.ini'))

# imagenes y hoja de estilo que el deck referencia por ruta relativa
for nombre in set(re.findall(r'[A-Za-z0-9_.-]+\.(?:png|jpg|jpeg|svg|gif|webp|css)',
                             io.open(AQUI / 'Presentacion.qmd', encoding='utf-8').read())):
    origen = AQUI / nombre
    if origen.exists():
        shutil.copy2(origen, DOCS / nombre)

peso = sum(f.stat().st_size for f in DOCS.rglob('*') if f.is_file()) / 1e6
notas = html.count('<aside class="notes"')
print(f'docs/ armado: {sum(1 for f in DOCS.rglob("*") if f.is_file())} archivos, {peso:.1f} MB')
print(f'notas del expositor publicadas: {notas}')
