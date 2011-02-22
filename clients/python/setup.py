from setuptools import setup

setup(name='voldemort',
      version='0.1.2',
      description='Python Voldemort Client',
      long_description=('Pure python client for accessing Voldemort key/value stores. ' +
                        'Supports both raw and JSON stores. Only supports the tcp protocol ' +
                        'buffer interface with server-side routing.'),
      packages=['voldemort', 'voldemort.protocol', 'voldemort.serialization'],
      author='LinkedIn Corporation',
      license='Apache 2.0',
      url='http://project-voldemort.com',
      install_requires=['protobuf>=2.3.0', 'simplejson>=2.1.1'],
      setup_requires=['nose>=0.11'],
)
