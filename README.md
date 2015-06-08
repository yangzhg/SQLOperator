# SQLOperator
an c++ wrapper of sql operation using variadic template
easily execute sql, using example:
usage:                                                                                          
operator.execute(                                                                             
"UPDATE user SET age = ? WHERE username = ?",                                             
ret_code,                                                                                 
age,                                                                                      
username);
