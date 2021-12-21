query_documento_clinico = '''
SELECT CD_OBJETO,
       cd_atendimento,
       cd_tipo_documento,
       tp_status,
       DH_CRIACAO
FROM PW_DOCUMENTO_CLINICO;
'''