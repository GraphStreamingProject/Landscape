#!/bin/bash

mkdir latex/images 2> /dev/null

# Create images
cd R_scripts
Rscript scaling_plot.R
mv scaling.png ../latex/images/scaling.png

Rscript dsu_query_plot.R
mv dsu_query.png ../latex/images/dsu_query.png

Rscript ablative_scaling_plot.R
mv ablative.png ../latex/images/ablative.png

# Create pdf document with images and tables
cd ../latex
pdflatex -synctex=1 -shell-escape -interaction=nonstopmode main.tex
mv main.pdf ../../figures.pdf

