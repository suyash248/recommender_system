import os

project_root = os.path.dirname(os.path.abspath(__file__)) # This is your Project Root

# bolt	        Use Bolt* protocol (None means autodetect)	bool,   None	None
# secure	    Use a secure connection (Bolt/TLS + HTTPS)	bool	False
# host	        Database server host name	                str	    'localhost'
# http_port	    Port for HTTP traffic	                    int	    7474
# https_port    Port for HTTPS traffic	                    int	    7473
# bolt_port	    Port for Bolt traffic	                    int	    7687
# user	        User to authenticate as	                    str	    'neo4j'
# password	    Password to use for authentication	        str	    no default
neo4j_config = {
    "password": "password"
}

{ "Id":   1,"similar": ["0804215715", "156101074X", "0687023955", "0687074231", "082721619X"]}