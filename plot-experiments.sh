#!/bin/bash

set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <timestamp>" >&2
  exit 1
fi

timestamp="$1"
measurements_dir="./experiments/$timestamp/measurements"
graphs_dir="./experiments/$timestamp/graphs"

if [[ ! -d "$measurements_dir" ]]; then
  echo "Measurements directory not found: $measurements_dir" >&2
  exit 1
fi

mkdir -p "$graphs_dir"

shopt -s nullglob
files=("$measurements_dir"/*)
shopt -u nullglob

if [[ ${#files[@]} -eq 0 ]]; then
  echo "No measurement files found in $measurements_dir" >&2
  exit 1
fi

for input_file in "${files[@]}"; do
  [[ -f "$input_file" ]] || continue

  header_line="$(head -n 1 "$input_file")"
  column_count="$(awk -F',' 'NR==1 { print NF; exit }' "$input_file")"

  base_name="$(basename "$input_file")"
  output_file="$graphs_dir/${base_name%.*}.pdf"
  plot_title="${base_name%.*}"
  x_label="$(printf '%s' "$header_line" | cut -d',' -f1 | sed 's/^[[:space:]]*//; s/[[:space:]]*$//')"
  y_label="Execution Time"
  summary_file="$(mktemp)"

  awk -F',' '
    function trim(s) {
      gsub(/^[[:space:]]+/, "", s)
      gsub(/[[:space:]]+$/, "", s)
      return s
    }
    NR == 1 { next }
    {
      worker = trim($1)
      n = 0
      sum = 0
      for (i = 2; i <= NF; i++) {
        v = trim($i)
        if (v == "") {
          continue
        }
        vals[++n] = v + 0
        sum += v + 0
      }

      if (n == 0) {
        next
      }

      mean = sum / n
      sq = 0
      for (i = 1; i <= n; i++) {
        d = vals[i] - mean
        sq += d * d
        delete vals[i]
      }
      stddev = (n > 1) ? sqrt(sq / (n - 1)) : 0

      printf "%s %.10g %.10g\n", worker, mean, stddev
    }
  ' "$input_file" > "$summary_file"

  gnuplot <<EOF
set terminal pdfcairo enhanced color font "Helvetica,10"
set output "$output_file"
set key top right
set grid
set title "$plot_title (Mean with Std Dev)"
set xlabel "$x_label"
set ylabel "$y_label"
set style fill solid 0.85 border rgb "#2f4b7c"
set boxwidth 0.9
bar_width = 0.9
half_bar = bar_width / 2.0
stddev_color = "#d62728"
plot "$summary_file" using 1:2 with boxes lc rgb "#4c78a8" title "Mean", \
     "$summary_file" using (\$1-half_bar):(\$2+\$3):(bar_width):(0) with vectors nohead lc rgb stddev_color lw 1.2 dashtype 2 title "Std Dev", \
     "$summary_file" using (\$1-half_bar):(\$2-\$3):(bar_width):(0) with vectors nohead lc rgb stddev_color lw 1.2 dashtype 2 notitle, \
     "$summary_file" using (\$1-half_bar):(\$2):(0):(\$3) with vectors nohead lc rgb stddev_color lw 1.0 notitle, \
     "$summary_file" using (\$1+half_bar):(\$2):(0):(\$3) with vectors nohead lc rgb stddev_color lw 1.0 notitle
EOF

  rm -f "$summary_file"
  echo "Created $output_file"
done

