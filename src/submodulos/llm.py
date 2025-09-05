
import os

from langchain_ollama.llms import OllamaLLM
from langchain_core.prompts import ChatPromptTemplate
from langchain_ollama import OllamaEmbeddings
from langchain_chroma import Chroma
from langchain_core.documents import Document

class VectorStoreLLM:
    """
        Clase para gestionar un flujo de trabajo entre un DataFrame, 
        un vector store persistente (Chroma) y un modelo de lenguaje (LLM) 
        utilizando embeddings y prompts.

        Esta clase convierte un DataFrame en documentos vectorizados, 
        los almacena en Chroma y permite consultas semánticas con recuperación 
        de contexto para alimentar a un LLM.

        Metodos:
        --------
            _agregar_documentos():
                Convierte el DataFrame en documentos y los agrega al vector store (Chroma).
            
            get_pregunta(pregunta: str) -> str:
                Realiza una búsqueda semántica sobre los documentos, 
                recupera el contexto y lo pasa al LLM junto con la pregunta.
    """
    def __init__(self, dataframe, config):
        """ 
            Funcion de __init__ la cual establece la configuracion para el LLM y el vector store.

            Args:
            -----
                config (dict): 
                    Configuración para LLM y vector store (colección, embeddings, etc.).
                df (pandas.DataFrame): 
                    DataFrame de entrada con los datos a vectorizar.
                collection_name (str): 
                    Nombre de la colección en Chroma.
                db_location (str): 
                    Ruta en disco donde se guarda la base de datos de Chroma.
                embedding_model (str): 
                    Nombre del modelo de embeddings usado con Ollama.
                k (int): 
                    Número de documentos a recuperar en cada búsqueda.
                llm_model (str): 
                    Nombre del modelo LLM de Ollama a utilizar.
                template (str): 
                    Plantilla de prompt para formatear las consultas.
            
            Side Effects:
            -----
                configuracion del LLM y vector store con el que trabajara el modelo.
        """

        self.config = config['llm']
        self.df = dataframe
        self.collection_name = self.config.get('collection_name')
        self.db_location = self.config.get('db_location')
        self.embedding_model = self.config.get('embedding_model')
        self.k = self.config.get('k')
        self.llm_model = self.config.get("llm_model")
        self.template = self.config.get("template")
        
        # Cargo el modelo de emdedding (que transforma en vector los datos)
        self.embeddings = OllamaEmbeddings(model=self.embedding_model)
        
        # Verifico si hay que agregar documentos
        self.add_documents = not os.path.exists(self.db_location)
        
        # Inicializo vector store
        self.vector_store = Chroma(
            collection_name=self.collection_name,
            persist_directory=self.db_location,
            embedding_function=self.embeddings
        )
        
        # Agrego documentos si la DB no existe
        if self.add_documents:
            self._agregar_documentos()
        
        self.retriever = self.vector_store.as_retriever(
            search_kwargs={"k": self.k}
        )

        # Inicializo LLM + prompt
        self.model = OllamaLLM(model=self.llm_model)
        self.prompt = ChatPromptTemplate.from_template(self.template)
        self.chain = self.prompt | self.model

    def _agregar_documentos(self): 
        """
            Convierte el DataFrame en una lista de documentos semánticos y los 
            agrega al vector store (Chroma) junto con sus metadatos.

            Cada fila del DataFrame se transforma en un objeto `Document`, 
            concatenando la información principal en `page_content` y 
            generando metadatos dinámicos a partir de la configuración.

            Side Effects:
                - Agrega los documentos al vector store (`self.vector_store`).
                - Imprime la cantidad de documentos insertados.
        """
        documents = []
        ids = []
        #page_content = self.config.get('page_content')
        metadata = self.config.get('metadata')

        self.df['ts'] = self.df["ts"].astype(str)

        for i, row in self.df.iterrows():
            
            # Construir el metadata dinámicamente
            meta = {k: row[v] for k, v in metadata.items()}
            
            doc = Document(
                page_content=f"El usuario {row['user_id']} realizó una compra por {row['price']} USD, "
                         f"el {row['ts']} y fue registrado por {row['updated_by']}.",
                metadata=meta,
                id=str(i)
            )
            documents.append(doc)
            ids.append(str(i))
        
        self.vector_store.add_documents(documents=documents, ids=ids)
        print(f"Se agregaron {len(documents)} documentos al vector store.")
    
    def get_pregunta(self, pregunta):
        """
            Realiza una consulta en lenguaje natural contra el vector store y devuelve 
            la respuesta generada por el modelo LLM.

            El método busca en el vector store los documentos más relevantes para la 
            `pregunta`, construye un contexto en texto plano a partir de sus metadatos, 
            y lo pasa junto con la pregunta al chain (prompt + LLM) para obtener la respuesta.

            Args:
                pregunta (str): Consulta en lenguaje natural realizada por el usuario.

            Returns:
                str: Respuesta generada por el LLM basada en la información recuperada.

        """
        
        reviews = self.retriever.invoke(pregunta)
        
        reviews_text = "\n".join(
            f"Usuario {r.metadata['user_id']} compró {r.metadata['precio']} USD "
            f"el {r.metadata['timestamp']} y fue actualizado por {r.metadata['updated_by']}" 
            for r in reviews
        )

        result = self.chain.invoke({
            "reviews": reviews_text, 
            "question": pregunta
        })
        
        return result
        
        
        