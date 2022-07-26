from setuptools import setup, find_packages

setup(
    name='pyrestsql',
    version='0.0.1',
    packages=find_packages(include=['pyrestsql', 'pyrestsql.*']),
    package_dir={'pyrestsql': 'pyrestsql'},
    package_data={
        'pyrestsql': [
            'swagger/templates/swagger/*.html',
            'swagger/static/css/*.css',
            'swagger/static/img/*.png',
            'swagger/static/js/*.js',
            'swagger/static/js/*.map',
        ]
    },
)
