Technical decision points for implementation are as follows:

1. Implementation of Validation
Validation class (Validator) is implemented to ensure the accuracy of data.
Implemented a test to validate the validity of the Validator.

2. Use multiprocessing.(Speed)
In order to minimize the loading time of csv files, we used multiprocessing.
10,000 files of data were used, and the execution time was compared with that of using list comprehensions and for statements.

3. Use django_pandas. (Flexibility)
For aggregation, we used django_pandas because pandas operations are more flexible in computing statistics than querysets operations.

4. Use plotly.(Flexibility)
We decided that dynamic graphs would be useful for users to analyze trends flexibly.
To generate dynamic graphs, we used Plotly as a library.
