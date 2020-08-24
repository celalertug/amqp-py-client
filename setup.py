from setuptools import setup, find_packages

classifiers = [
    'License :: OSI Approved :: MIT License',
    'Programming Language :: Python :: 3'
]

setup(
    name='amqp_py_client',
    version='0.0.1',
    description='simple rpc amqp python library on pike',
    long_description=open('readme.txt').read(),
    url='',
    author='celal ertuğ',
    author_email='celalertug@gmail.com',
    license='MIT',
    classifiers=classifiers,
    keywords='amqp_client',
    packages=find_packages(),
    install_requires=['pika']
)
