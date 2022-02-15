from src.pois_processor.etl.extractor import Extractor


if __name__ == "__main__":
    data_path = "../../data/90per"
    extractor = Extractor(data_path)
    extractor.extract_data()
