## <b>UIC Data Processing Workshop II - Search Engine Demo (My Book Searcher</b>

### HomePage
![avatar](/imgs/homepage.jpg)

### Search Result Page
![avatar](/imgs/result_page.jpg)

### Source Data
We collected 60,000 E-Books from open source website http://www.gutenberg.org/

### Index and Ranking
We used Hadoop to create the inversed indexing for each word/phase and TF-IDF algorithm is adopted to rank

### System
Django is adopted to create the search engine demo

### File Structure

### .idea
> The Pycharm configuration files

### code_nondjango

> * get_set_of_word 
>> a .ipynb file to get the set of the union of word to perform input correction function

> * hadoop_index
>> files to get the inverted index and TF-IDF value

> * lucene_index
>> files to get the inverted idnex by combining lucene and hadoop

>* python_index
>> python files to get the inverted index

>* web_crawling
>> python files to download text files from the Internet and perform cleaning.

### venv
> Pycharm virtual environment settings

### ws2_django_demo
> The web server and interface, with the source text files in the static/
