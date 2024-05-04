# automated-document-recognition
Document recognition is the process of understanding document contact to determine a topic under which the document can be categorized.\
Many organizations have a team dedicated to the organization of documents into a semi-structured index consisting of a category.\
An example of this is in the logistics and supply chain industry where document recognition is used to extract and classify bills of landing, invoices, shipping labels, customs paperwork, and compliance certifications.\
While a manual process of categorizing documents works when the volume of data is small, there is a need for an AI-driven automated document recognition (ADR) that is needed to make categorization economical at large scale. 

# Artificial Intelligence based Document Recognition
Trained machine learning models are able to accurately categorize documents and streamline workflows for many industries.\
Using artificial intelligence, companies can see a 10x increase in productivity of human in the loop document categorization workflows. 


# Training a new model
To train a document categorization model requires training data. A new training job can be kicked off by running train.py with the path to the folder where training data is located. \
Training data is categorized into folders. The folder name is the category name. The documents inside the folder can be any format.

The dataset has to be stored with the following directory structure

Top Level Directory
  Label1 
    file1 
    file2 
  Label2 
    file1 
    file2
... 

Basically, the folder name is the label and inside the folder are pdf files. 