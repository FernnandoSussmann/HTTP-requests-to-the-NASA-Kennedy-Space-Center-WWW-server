# HTTP-requests-to-the-NASA-Kennedy-Space-Center-WWW-server
Job interview test. Read the "how to use" part before trying to run the code.
Teste de entrevista de emprego. Leia a sessão de "como usar" antes de rodar o código. 

## Requirements / Requisitos
Python*
Pyspark library (pip install pyspark)
Pandas library (pip install pandas)

*If you are windows user make sure to enable python on command line when installing it./ Se você utiliza windows tenha certeza que habilitou a opção de rodar python no prompt de comando ao realizar a instalação.

## How to use
Files on the original_data folder have to be extracted on the aplication's root folder.
It is necessary to delete the last line of access_log_Jul95 because it is incomplete and seems useless.
If you want to redownload the data you can run wget_data.sh or wget_data.bat.
Make sure you can run spark code on command line. (Requirements)
Then run "python main.py" on command line and see it working.
After the execution some csv files will be gerated by the code.

## Como usar
Os arquivos na pasta original_data precisam ser extraídos para a pasta raiz da aplicação.
É necessário deletar a ultima linha do arquivo access_log_Jul95 pois a informação está incompleta e não parece ser útil.
Se quiser baixar novamente os dados originais pode utilizar os scripts wget_data.sh ou wget_data.bat.
Tenha certeza que consegue rodar o spark por linha de comando. (Requisitos)
Então rode "python main.py" no terminal e veja acompanhe o funcionamento.
Após a execução haverá alguns arquivos csv gerados pelo código.

As perguntas e as respostas das questões referentes a esse programa estão no arquivo "Perguntas_respondidas.docx".