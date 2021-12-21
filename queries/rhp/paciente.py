query_paciente = '''
SELECT DISTINCT 
      P.CD_PACIENTE,
      DT_NASCIMENTO,
      TP_SEXO,
      DT_CADASTRO,
      NM_BAIRRO
FROM PACIENTE P;
'''