(ns query-server.conf)

  
  (defn get-config
	[conf-file]
 (read-string (slurp conf-file))
    
  )
  
