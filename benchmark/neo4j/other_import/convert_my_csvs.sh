csv_dir=${NEO4J_CONTAINER_ROOT}/import/
header_file=my_headers.txt

cat $header_file | while read line
do
	file_name=$(echo $line | awk '{print $1}')
	header=$(echo $line | awk '{print $2}')
	sed -i "1i  ${header}" ${csv_dir}$file_name
done



