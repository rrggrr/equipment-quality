import keyring, sys
import pandas as pd

#Get Passwords

def gc():
    #account scope
    hc_scope = "rustin"

    credentials = {
    'myd' : keyring.get_password("mysqldatabase", hc_scope),
    'mysql_database' : keyring.get_password("mysqldatabase", hc_scope),
    'sql_database_name' : keyring.get_password("mysqldatabase", hc_scope),
    #
    'mydp' : keyring.get_password("mysqldatabasepassword", hc_scope),
    'mysql_database_password' : keyring.get_password("mysqldatabasepassword", hc_scope),
    'mysql_databasepassword' : keyring.get_password("mysqldatabasepassword", hc_scope),
    'sql_database_password' : keyring.get_password("mysqldatabasepassword", hc_scope),
    #
    'mydu' : keyring.get_password("mysqldatabaseuser", hc_scope),
    'mysql_database_user' : keyring.get_password("mysqldatabaseuser", hc_scope),
    'sql_database_user' : keyring.get_password("mysqldatabaseuser", hc_scope),
    #
    'mydsmd' : keyring.get_password("salesmgrdatabase", hc_scope),
    'mysql_salesmanager_database' : keyring.get_password("salesmgrdatabase", hc_scope),
    'mysqlsalesmanagerdatabase' : keyring.get_password("salesmgrdatabase", hc_scope),
    #
    'mydaws' : keyring.get_password("awslocation", hc_scope),
    'aws_location' : keyring.get_password("awslocation", hc_scope),
    'mysql_aws_location' : keyring.get_password("awslocation", hc_scope),
    #
    'mydawsu' : keyring.get_password("awssshuser", hc_scope),
    'mysql_aws_user' : keyring.get_password("awssshuser", hc_scope),
    'aws_username' : keyring.get_password("awssshuser", hc_scope),
    #
    'local_dir' : keyring.get_password("localdirectory", hc_scope),
    'localdir' : keyring.get_password("localdirectory", hc_scope),
    'output_dir' : keyring.get_password("localdirectory", hc_scope),
    #
    'target' : keyring.get_password("armcoservertarget", hc_scope),
    'armco_server_address' : keyring.get_password("armcoservertarget", hc_scope),
    #
    'pvtpath' : keyring.get_password("pvtpath", hc_scope),
    'xerokey' : keyring.get_password("xeroconsumerkey", hc_scope),
    #
    'mix' : keyring.get_password("mixmax", hc_scope),
    'mixmaxkey' : keyring.get_password("mixmax", hc_scope),
    #
    'qdir' : keyring.get_password("quotedir", hc_scope),
    'quote_dir' : keyring.get_password("quotedir", hc_scope),
    'quotedir' : keyring.get_password("quotedir", hc_scope)
    }



    return credentials

if __name__ == "__main__":
    g = gc()
    if len(sys.argv) > 1:
        print(sys.argv)
        if sys.argv[1] == 'test':
            gpd = pd.DataFrame.from_dict(g, orient='index')
            #outputpath = keyring.get_password("localdirectory", 'XXXXX') + 'getCredentials.csv'
            #gpd.to_csv(path_or_buf=outputpath)
            print(gpd)
            #print("*** CSV output saved to" + outputpath)
        else:
            print("Wrong command line argument supplied")
    else:
        pass
