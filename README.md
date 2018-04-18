# HTTP-requests-to-the-NASA-Kennedy-Space-Center-WWW-server
Job interview test. Read the "how to use" part before trying to run the code. <br />
Teste de entrevista de emprego. Leia a sessão de "como usar" antes de rodar o código. <br /> 

## Requirements / Requisitos
Python* <br />
Pyspark library (pip install pyspark) <br />
Pandas library (pip install pandas) <br /> <br />

*If you are windows user make sure to enable python on command line when installing it./ Se você utiliza windows tenha certeza que habilitou a opção de rodar python no prompt de comando ao realizar a instalação. <br /> <br />

## How to use
Files on the original_data folder have to be extracted on the aplication's root folder. <br />
It is necessary to delete the last line of access_log_Jul95 because it is incomplete and seems useless. <br />
If you want to redownload the data you can run wget_data.sh or wget_data.bat. <br />
Make sure you can run spark code on command line. (Requirements) <br />
Then run "python main.py" on command line and see it working. <br />
After the execution some csv files will be gerated by the code. <br />

## Como usar
Os arquivos na pasta original_data precisam ser extraídos para a pasta raiz da aplicação. <br />
É necessário deletar a ultima linha do arquivo access_log_Jul95 pois a informação está incompleta e não parece ser útil. <br />
Se quiser baixar novamente os dados originais pode utilizar os scripts wget_data.sh ou wget_data.bat. <br />
Tenha certeza que consegue rodar o spark por linha de comando. (Requisitos) <br /> 
Então rode "python main.py" no terminal e veja acompanhe o funcionamento. <br />
Após a execução haverá alguns arquivos csv gerados pelo código. <br />

As perguntas e as respostas das questões referentes a esse programa estão no arquivo "Perguntas_respondidas.docx". <br />

### Running on local spark installation
You can also run on a local spark instalation. But you have to modify some parts of the code: <br />
-You have to modify 77 and 78 adding project's path before the file's name. (Example /home/project/access_log_Aug95) <br />
-You also have to modify lines 95, 101, 107, 113 and 119 adding a new path for saving the documents or otherwise it will save on spark folder. <br />
After it is done you have to run on you "spark-2.2.1-bin-hadoop2.7/bin" folder "./spark-submit projectpath/main.py" and it will start executing <br />

### Executando em uma instalação local do spark
Você também pode rodar em uma instalação local do spark, caso possuir. Para isso você precisará alterar algumas partes do código:  <br />
-Você precisa alterar as linhas 77 e 78 adicionando o local do projeto antes do nome do arquivo que será carregado. (Exemplo /home/project/access_log_Aug95)  <br />
-Você também precisa alterar as linhas 95, 101, 107, 113 e 119 adicionando um novo local para salvar os arquivos gerados. Caso não alterar ele salvará os arquivos na pasta do spark. <br />
Após ter executado essas ações você só precisa rodar na pasta "spark-2.2.1-bin-hadoop2.7/bin" o comando "./spark-submit pastadoprojeto/main.py" e iniciará a execução. <br />