#!/bin/bash
# Compile README.md to DIN A4 PDF using pandoc + xelatex

set -e

INPUT="exam_questions.md"
OUTPUT="exam_questions.pdf"

pandoc "$INPUT" \
  -o "$OUTPUT" \
  --pdf-engine=xelatex \
  -V geometry:a4paper \
  -V geometry:margin=2.5cm \
  -V fontsize=11pt \
  -V lang=de-DE \
  --syntax-highlighting=tango \
  -V colorlinks=true \
  -V linkcolor=blue \
  -V urlcolor=blue

echo "✓ Generated $OUTPUT"
