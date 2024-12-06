import hashlib

class KademilaFile:
    '''
    A class that represents a kad-file.
    '''
    def __init__(self, dict_kad_file: dict):
        self.construct_kad_file(dict_kad_file)
    
    def construct_kad_file(self, dict_kad_file: dict):
        '''
        Constructs a kad-file from a dictionary.
        '''
        if not self.correct_format(dict_kad_file):
            raise ValueError('Incorrect format for kad-file')
        
        self.version     = dict_kad_file['version']
        self.song_name   = dict_kad_file['song_name']
        self.artist_name = dict_kad_file['artist_name']
        self.providers   = dict_kad_file['providers']
        self.id          = dict_kad_file["id"]
    
    def correct_format(self, dict_kad_file: dict) -> bool:
        '''
        Checks if all the correct fields are present in the kad-file.
        '''
        # Check if version, song_name, song_artist, and providers are present
        fields = ['version', 'song_name', 'artist_name', 'providers']
        return all(field in dict_kad_file for field in fields)
    
    def add_provider(self, provider):
        '''
        Adds a provider server addr to the kad-file.
        '''
        self.providers.append((provider.node.id, (provider.node.ip, provider.node.port)))
        self.version += 1

    @property
    def key(self) -> int:
        '''
        The hashed key of the song id.
        '''
        return int(hashlib.sha1(self.song_id.encode()).hexdigest(), 16)
    
    @property
    def dict(self) -> dict:
        '''
        Returns a dictionary representation of the kad-file.
        '''
        return {
            'version'    : self.version,
            'song_name'  : self.song_name,
            'artist_name': self.artist_name,
            'providers'  : self.providers,
            'id'         : self.id
        }
    
    @property
    def song_id(self) -> str:
        return f"{self.song_name}-{self.artist_name}"
    
    def __repr__(self):
        string = []
        string.append(f" Version     : {self.version}\n")
        string.append(f"\t\tSong Name   : {self.song_name}\n")
        string.append(f"\t\tArtist Name : {self.artist_name}\n")
        _id = self.id if len(str(self.id)) <= 10 else str(self.id)[:10] + '...'
        string.append(f"\t\tSong ID     : {_id}\n")
        string.append(f"\t\tProviders   : \n")
        for provider in self.providers:
            string.append(f"\t\t\t{provider}\n")
        return ''.join(string)