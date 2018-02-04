class Scraper:
    def refresh_movie(self, movie):
        # Check that a valid movie was supplied
        try:
            _ = movie.imdbid
        except AttributeError:
            print("Invalid movie supplied")
            return None

