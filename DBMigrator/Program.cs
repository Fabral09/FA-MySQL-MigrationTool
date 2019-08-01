using System;
using System.Linq;
using System.IO;
using System.Configuration;
using MySql.Data.MySqlClient;

namespace DBMigrator
{
    enum Direction
    {
        UP,
        DOWN
    }

    class Program
    {
        static void Main(string[] args)
        {
            string server = string.Empty;
            string db = string.Empty;
            string user = string.Empty;
            string password = string.Empty;

            int currentVersion = 0;
            int versionToReach = 0;
            Direction directionToRead = Direction.UP;
            try
            {
                if (ConfigurationManager.AppSettings["currentVersion"] != null)
                {
                    string currentVersionStr = ConfigurationManager.AppSettings["currentVersion"].ToString();

                    if (!int.TryParse(currentVersionStr, out currentVersion))
                    {
                        Console.WriteLine("Impossibile leggere la versione corrente!");
                        throw new Exception("Impossibile leggere la versione corrente!");
                    }
                    else
                    {
                        if ( args.Length != 2 )
                        {
                            Console.WriteLine("Errore nell'utilizzo da riga di comando! Es: dbmigrator goto VERSION");
                            throw new Exception("Errore nell'utilizzo da riga di comando! Es: dbmigrator go_to VERSION");
                        }

                        if (!int.TryParse(args[1], out versionToReach))
                        {
                            Console.WriteLine("Impossibile leggere la versione da raggiungere!");
                            throw new Exception("Impossibile leggere la versione da raggiungere!");
                        }
                        else
                        {
                            if (currentVersion < versionToReach)
                            {
                                directionToRead = Direction.UP;
                            }
                            else
                            {
                                directionToRead = Direction.DOWN;
                            }
                        }
                    }
                }
            }
            catch (Exception)
            {
                Environment.Exit(1);
            }

            if ( string.IsNullOrEmpty( ConfigurationManager.AppSettings["server"] ) )
            {
                Console.WriteLine("Indirizzo del server db specificato non valido!");
                Environment.Exit(1);
            }
            else
            {
                server = ConfigurationManager.AppSettings["server"];
            }

            if (string.IsNullOrEmpty(ConfigurationManager.AppSettings["db"]))
            {
                Console.WriteLine("Nome del db specificato non valido!");
                Environment.Exit(1);
            }
            else
            {
                db = ConfigurationManager.AppSettings["db"];
            }

            if (string.IsNullOrEmpty(ConfigurationManager.AppSettings["user"]))
            {
                Console.WriteLine("Utente db specificato non valido!");
                Environment.Exit(1);
            }
            else
            {
                user = ConfigurationManager.AppSettings["user"];
            }

            if ( ConfigurationManager.AppSettings["password"] == null )
            {
                Console.WriteLine("Password del db specificato non valida!");
                Environment.Exit(1);
            }
            else
            {
                password = ConfigurationManager.AppSettings["password"];
            }

            MySqlConnectionStringBuilder connectionString = new MySqlConnectionStringBuilder();
            connectionString.Server = server;
            connectionString.Database = db;
            connectionString.UserID = user;
            connectionString.Password = password;

            using (MySqlConnection conn = new MySqlConnection(connectionString.ConnectionString))
            {
                try
                {
                    conn.Open();
                }
                catch (Exception)
                {
                    Console.WriteLine("Errore nell'apertura della connessione");
                    Environment.Exit(1);
                }

                string[] files = null;
                if (directionToRead == Direction.UP)
                {
                    files = Directory.GetFiles(string.Format("{0}\\query", Directory.GetCurrentDirectory())).OrderBy(f => int.Parse(Path.GetFileNameWithoutExtension(f))).ToArray<string>();
                }
                else
                {
                    files = Directory.GetFiles(string.Format("{0}\\query", Directory.GetCurrentDirectory())).OrderBy(f => int.Parse(Path.GetFileNameWithoutExtension(f))).Reverse().ToArray<string>();
                }

                MySqlCommand cmd = new MySqlCommand();
                cmd.Connection = conn;
                cmd.CommandType = System.Data.CommandType.Text;

                foreach (var currentFile in files)
                {
                    string fileNameWithoutExtension = Path.GetFileNameWithoutExtension(currentFile);
                    int versionToCheck = 0;

                    if ( !int.TryParse( fileNameWithoutExtension, out versionToCheck ) )
                    {
                        Console.WriteLine("Formato del nome del file non valido!");
                        Environment.Exit(1);
                    }
                    else
                    {
                        if ( (directionToRead == Direction.DOWN && versionToCheck <= versionToReach) ||
                             (directionToRead == Direction.UP && versionToCheck > versionToReach))
                        {
                            break;
                        }
                    }

                    if ( (versionToCheck > currentVersion) && directionToRead == Direction.DOWN)
                    {
                        continue;
                    }

                    if ( (currentVersion == versionToCheck) && directionToRead == Direction.UP)
                    {
                        continue;
                    }

                    Console.WriteLine("FILE IN LAVORAZIONE: {0}", currentFile);
                    try
                    {
                        using (StreamReader reader = new StreamReader(currentFile))
                        {
                            try
                            {
                                while (!reader.EndOfStream)
                                {
                                    string currentLine = reader.ReadLine();

                                    if (directionToRead == Direction.UP)
                                    {
                                        if (currentLine.StartsWith("+"))
                                        {
                                            currentLine = currentLine.Replace("+", string.Empty);
                                            cmd.CommandText = currentLine;
                                            cmd.ExecuteNonQuery();
                                            Console.WriteLine("QUERY IN UPGRADE: {0}", currentLine);
                                        }
                                    }
                                    else if (directionToRead == Direction.DOWN)
                                    {
                                        if (currentLine.StartsWith("-"))
                                        {
                                            currentLine = currentLine.Replace("-", string.Empty);
                                            cmd.CommandText = currentLine;
                                            cmd.ExecuteNonQuery();
                                            Console.WriteLine("QUERY IN DOWNGRADE: {0}", currentLine);
                                        }
                                    }
                                }                                
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine("Errore nella lettura del file");
                                throw;
                            }
                            finally
                            {
                                reader.Close();
                            }
                        }
                    }
                    catch (Exception)
                    {
                        Console.WriteLine("Errore nell'utilizzo del file");
                        Environment.Exit(1);
                    }
                }

                try
                {
                    if (conn.State == System.Data.ConnectionState.Open)
                    {
                        conn.Close();
                    }
                }
                catch (Exception)
                {
                    Console.WriteLine("Errore nella chiusura della connessione");
                    Environment.Exit(1);
                }
            }
        }
    }
}
