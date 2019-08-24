cd %~dp0

cythonize -3 -i *.pyx

sleep 3

del *.c
rmdir /s /q tmp*
