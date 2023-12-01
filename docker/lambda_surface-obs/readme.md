## testing


docker build -t wis2monitoring_bufrsurface .

docker run -v C:\Users\Timo\Documents\git\wis2monitoring\docker\lambda_surface-obs\app\test.py:/function/test.py  -v C:\Users\Timo\Documents\git\wis2monitoring\docker\lambda_surface-obs\test\:/test_data  -it --entrypoint /bin/bash wis2monitoring_bufrsurface