for py in *.py
do
  /opt/anaconda3/bin/python3 -m py_compile ${py} || exit 1
done