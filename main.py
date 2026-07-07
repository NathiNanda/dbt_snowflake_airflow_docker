from src.ingest import extract_and_load

def main():
    print("Iniciando processo de ingestão de câmbio para o S3...")
    extract_and_load()

if __name__ == "__main__":
    main()

