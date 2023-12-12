from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(name='db_sync',
      version='0.0.1',
      description='cross-database table synchronisation and replication',
      long_description=long_description,
      long_description_content_type="text/markdown",
      author='Dragan Matesic',
      author_email='reach me on LinkedIn dont spam my inbox',
      license='MIT',
      packages=find_packages(),
      zip_safe=False,
      include_package_data=True,
      install_requires=['sqlalchemy', 'psycopg2', 'pymssql', 'cx_oracle',
                        'python-dotenv'],
      scripts=[]
      )
