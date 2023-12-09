import cfbd

def create_api_client() -> cfbd.ApiClient:
    config = cfbd.Configuration()
    
    with open("key", "r") as f:
        config.api_key['Authorization'] = f.read().strip()
        config.api_key_prefix['Authorization'] = 'Bearer'
    
    return cfbd.ApiClient(config)
