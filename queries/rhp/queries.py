query_atendime = 'SELECT A.CD_MULTI_EMPRESA, CD_PACIENTE, CD_ATENDIMENTO, CD_CID, CD_MOT_ALT, CD_TIP_RES, CD_CONVENIO, CD_ESPECIALID, CD_PRESTADOR, CD_ATENDIMENTO_PAI, CD_LEITO, A.CD_ORI_ATE, CD_SERVICO, TP_ATENDIMENTO, DT_ATENDIMENTO, HR_ATENDIMENTO, HR_ALTA, HR_ALTA_MEDICA, CD_TIP_MAR, CD_SINTOMA_AVALIACAO, NM_USUARIO_ALTA_MEDICA FROM DBAMV.VW_EXIMIO_ATENDIME A'

query_cid = 'SELECT CD_CID, DS_CID, CD_SGRU_CID FROM DBAMV.VW_EXIMIO_CID'

query_classificacao_risco = 'SELECT CD_CLASSIFICACAO_RISCO, CD_COR_REFERENCIA, CD_TRIAGEM_ATENDIMENTO, DH_CLASSIFICACAO_RISCO FROM DBAMV.VW_EXIMIO_SACR_CLASS_RISCO'

query_classificacao = 'SELECT CD_CLASSIFICACAO, DS_TIPO_RISCO FROM DBAMV.VW_EXIMIO_SACR_CLASSIFICACAO'

query_convenio = 'SELECT CD_CONVENIO, NM_CONVENIO FROM DBAMV.VW_EXIMIO_CONVENIO'

query_cor_referencia = 'SELECT CD_COR_REFERENCIA FROM DBAMV.VW_EXIMIO_SACR_COR_REFERENCIA'

query_diagnostico_atendime = 'SELECT CD_CID, CD_DIAGNOSTICO_ATENDIME, CD_ATENDIMENTO FROM DBAMV.VW_EXIMIO_DIAGNOSTICO_ATENDIME'

query_documento_clinico = 'SELECT CD_OBJETO, cd_atendimento, cd_tipo_documento, tp_status, DH_CRIACAO FROM DBAMV.VW_EXIMIO_PW_DOCUMENTO_CLINICO'

query_esp_med = 'SELECT CD_ESPECIALID, CD_PRESTADOR, SN_ESPECIAL_PRINCIPAL FROM DBAMV.VW_EXIMIO_ESP_MED'

query_especialidad = 'SELECT CD_ESPECIALID, DS_ESPECIALID FROM DBAMV.VW_EXIMIO_ESPECIALID'

query_gru_cid = 'SELECT CD_GRU_CID, DS_GRU_CID FROM DBAMV.VW_EXIMIO_GRU_CID'

query_mot_alt = 'SELECT CD_MOT_ALT, DS_MOT_ALT, TP_MOT_ALTA FROM DBAMV.VW_EXIMIO_MOT_ALT'

query_multi_empresa = 'SELECT CD_MULTI_EMPRESA, DS_MULTI_EMPRESA FROM DBAMV.VW_EXIMIO_MULTI_EMPRESAS'

query_ori_ate = 'SELECT CD_ORI_ATE, DS_ORI_ATE, TP_ORIGEM, CD_SETOR FROM DBAMV.VW_EXIMIO_ORI_ATE'

query_paciente = 'SELECT DISTINCT P.CD_PACIENTE, DT_NASCIMENTO, TP_SEXO, DT_CADASTRO, NM_BAIRRO FROM DBAMV.VW_EXIMIO_PACIENTE P'

query_pagu_objeto = 'SELECT CD_OBJETO, TP_OBJETO FROM DBAMV.VW_EXIMIO_PAGU_OBJETO'

query_registro_alta = 'SELECT HR_ALTA_MEDICA, CD_ATENDIMENTO FROM DBAMV.VW_EXIMIO_PW_REGISTRO_ALTA'

query_setor = 'SELECT CD_SETOR, NM_SETOR FROM DBAMV.VW_EXIMIO_SETOR'

query_sgru_cid = 'SELECT CD_SGRU_CID, CD_GRU_CID, DS_SGRU_CID FROM DBAMV.VW_EXIMIO_SGRU_CID'

query_sintoma_avaliacao = 'SELECT CD_SINTOMA_AVALIACAO, DS_SINTOMA FROM DBAMV.VW_EXIMIO_SACR_SINTOMA_AV'

query_tempo_processo = 'SELECT DH_PROCESSO,  CD_TIPO_TEMPO_PROCESSO,  CD_ATENDIMENTO FROM DBAMV.VW_EXIMIO_SACR_TEMPO_PROCESSO'

query_tip_mar = 'SELECT CD_TIP_MAR FROM DBAMV.VW_EXIMIO_TIP_MAR'

query_tip_res = 'SELECT CD_TIP_RES, DS_TIP_RES, SN_OBITO FROM DBAMV.VW_EXIMIO_TIP_RES'

query_triagem_atendimento = 'SELECT CD_ATENDIMENTO, CD_TRIAGEM_ATENDIMENTO, CD_SINTOMA_AVALIACAO, DS_SENHA, DH_PRE_ATENDIMENTO FROM DBAMV.VW_EXIMIO_TRIAGEM_ATENDIMENTO'

query_usuario = 'SELECT CD_USUARIO, NM_USUARIO FROM DBASGU.VW_EXIMIO_USUARIOS'

query_pre_med = 'SELECT CD_PRE_MED, PM.CD_ATENDIMENTO, PM.CD_PRESTADOR, CD_DOCUMENTO_CLINICO, DT_PRE_MED, TP_PRE_MED, PM.CD_SETOR FROM DBAMV.VW_EXIMIO_PRE_MED'

query_itpre_med = 'SELECT IP.CD_PRE_MED, CD_ITPRE_MED, CD_PRODUTO, CD_TIP_PRESC, CD_TIP_ESQ, CD_FOR_APL, CD_TIP_FRE, TP_JUSTIFICATIVA FROM DBAMV.VW_EXIMIO_ITPRE_MED'

query_tip_presc = 'SELECT TP.CD_TIP_PRESC, DS_TIP_PRESC, CD_PRO_FAT FROM DBAMV.VW_EXIMIO_TIP_PRESC'

query_for_apl = 'SELECT CD_FOR_APL, DS_FOR_APL FROM DBAMV.VW_EXIMIO_FOR_APL'

query_tip_esq = 'SELECT CD_TIP_ESQ, DS_TIP_ESQ FROM DBAMV.VW_EXIMIO_TIP_ESQ'

query_tip_fre = 'SELECT CD_TIP_FRE, DS_TIP_FRE FROM DBAMV.VW_EXIMIO_TIP_FRE'

query_de_para_tuss = 'SELECT CNHPM_5, CBHPM_4, CNHPM_3, AMB_1990, AMB_1992, AMB_1996, AMB_1999, TUSS FROM DBAMV.VW_EXIMIO_DE_PARA_TUSS'

query_gru_fat = 'SELECT CD_GRU_FAT, DS_GRU_FAT, TP_GRU_FAT FROM DBAMV.VW_EXIMIO_GRU_FAT'

query_gru_pro = 'SELECT CD_GRU_PRO, CD_GRU_FAT, DS_GRU_PRO, TP_GRU_PRO, TP_CUSTO FROM DBAMV.VW_EXIMIO_GRU_PRO'

query_produto = 'SELECT CD_PRO_FAT, DS_PRODUTO, CD_PRODUTO, VL_FATOR_PRO_FAT, SN_OPME, CD_ESPECIE, VL_CUSTO_MEDIO FROM DBAMV.VW_EXIMIO_PRODUTO'

query_pro_fat = 'SELECT CD_GRU_PRO, CD_PRO_FAT, CD_POR_ANE, DS_PRO_FAT FROM DBAMV.VW_EXIMIO_PRO_FAT'

query_tuss = 'SELECT CD_PRO_FAT, CD_TUSS, DS_TUSS FROM DBAMV.VW_EXIMIO_TUSS'

query_uni_pro = 'SELECT CD_UNIDADE, DS_UNIDADE, VL_FATOR, TP_RELATORIOS, CD_UNI_PRO, CD_PRODUTO, SN_ATIVO FROM DBAMV.VW_EXIMIO_UNI_PRO'

query_reg_amb = 'SELECT DISTINCT RA.CD_REG_AMB, CD_REMESSA, RA.VL_TOTAL_CONTA FROM DBAMV.VW_EXIMIO_REG_AMB'

query_itreg_amb = 'SELECT IRA.CD_ATENDIMENTO, CD_PRO_FAT, CD_REG_AMB, CD_GRU_FAT, CD_LANCAMENTO, QT_LANCAMENTO, VL_UNITARIO, VL_NOTA, IRA.CD_SETOR, CD_SETOR_PRODUZIU, TP_PAGAMENTO, SN_PERTENCE_PACOTE, VL_TOTAL_CONTA, SN_FECHADA, DT_FECHAMENTO, CD_ITMVTO FROM DBAMV.VW_EXIMIO_ITREG_AMB IRA'

query_reg_fat = 'SELECT CD_REG_FAT, SN_FECHADA, DT_INICIO, DT_FINAL, DT_FECHAMENTO, CD_REMESSA, VL_TOTAL_CONTA, RF.CD_ATENDIMENTO FROM DBAMV.VW_EXIMIO_REG_FAT RF'

query_itreg_fat = 'SELECT IRF.CD_REG_FAT, CD_LANCAMENTO, DT_LANCAMENTO, QT_LANCAMENTO, TP_PAGAMENTO, VL_UNITARIO, VL_NOTA, IRF.CD_CONTA_PAI, CD_PRO_FAT, CD_GRU_FAT, IRF.VL_TOTAL_CONTA, SN_PERTENCE_PACOTE, IRF.CD_SETOR, CD_SETOR_PRODUZIU, CD_ITMVTO FROM DBAMV.VW_EXIMIO_ITREG_FAT IRF'

query_custo_final = 'SELECT VL_CUSTO_CENCIR, DT_COMPETENCIA FROM DBAMV.VW_EXIMIO_CUSTO_FINAL'

query_mvto_estoque = 'SELECT CD_MVTO_ESTOQUE, ME.CD_SETOR, ME.CD_ATENDIMENTO, CD_MOT_DEV, ME.CD_MULTI_EMPRESA, DT_MVTO_ESTOQUE FROM DBAMV.VW_EXIMIO_MVTO_ESTOQUE ME'

query_itmvto_estoque = 'SELECT CD_ITMVTO_ESTOQUE, QT_MOVIMENTACAO, IME.CD_MVTO_ESTOQUE, CD_PRODUTO, IME.CD_UNI_PRO FROM DBAMV.VW_EXIMIO_ITMVTO_ESTOQUE IME'

query_quantidade_diarias = 'SELECT QD.CD_ATENDIMENTO, VL_DIARIA, QTD_DIARIAS FROM DBAMV.VW_EXIMIO_QUANTIDADE_DIARIAS QD'

query_remessa_fatura = 'SELECT RF.CD_REMESSA, DT_ABERTURA, RF.DT_FECHAMENTO, DT_ENTREGA_DA_FATURA FROM DBAMV.VW_EXIMIO_REMESSA_FATURA RF'

query_repasse = 'SELECT DISTINCT R.CD_REPASSE, DT_COMPETENCIA FROM DBAMV.VW_EXIMIO_REPASSE R'

query_it_repasse = 'SELECT DISTINCT ITR.CD_REG_FAT, CD_LANCAMENTO_FAT, CD_REPASSE FROM DBAMV.VW_EXIMIO_IT_REPASSE ITR'

query_itent_pro = 'SELECT VL_TOTAL, ITP.CD_ATENDIMENTO, CD_PRODUTO, VL_UNITARIO, DT_GRAVACAO FROM DBAMV.VW_EXIMIO_ITENT_PRO ITP'

query_glosas = 'SELECT DISTINCT CD_GLOSAS, CD_REG_FAT, G.CD_REG_AMB, G.CD_MOTIVO_GLOSA, VL_GLOSA, CD_LANCAMENTO_FAT, CD_LANCAMENTO_AMB FROM DBAMV.VW_EXIMIO_GLOSAS G'

query_custo_medio_mensal = 'SELECT VL_CUSTO_MEDIO, DH_CUSTO_MEDIO, CD_PRODUTO, CD_MULTI_EMPRESA FROM DBAMV.VW_EXIMIO_CUSTO_MEDIO_MENSAL'

query_fa_custo_atendimento = 'SELECT VL_DIARIA, VL_CUSTO_GASES, VL_CUSTO_REPASSE, VL_CUSTO_MEDICAMENTO, VL_PROCEDIMENTO, VL_CUSTO_DIARIATAXA, FCA.CD_ATENDIMENTO FROM DBAMV.VW_EXIMIO_FA_CUSTO_ATENDIMENTO FCA'

query_especie = 'SELECT CD_ESPECIE, DS_ESPECIE FROM DBAMV.VW_EXIMIO_ESPECIE'

query_exa_lab = 'SELECT CD_PRO_FAT, CD_EXA_LAB, NM_EXA_LAB FROM DBAMV.VW_EXIMIO_EXA_LAB'

query_exa_rx = 'SELECT EXA_RX_CD_PRO_FAT, CD_EXA_RX, DS_EXA_RX FROM DBAMV.VW_EXIMIO_EXA_RX'

query_gru_fat = 'SELECT CD_GRU_FAT, DS_GRU_FAT, TP_GRU_FAT FROM DBAMV.VW_EXIMIO_GRU_FAT'

query_gru_pro = 'SELECT CD_GRU_PRO, CD_GRU_FAT, DS_GRU_PRO, TP_GRU_PRO, TP_CUSTO FROM DBAMV.VW_EXIMIO_GRU_PRO'

query_motivo_glosa = 'SELECT DS_MOTIVO_GLOSA, CD_MOTIVO_GLOSA FROM DBAMV.VW_EXIMIO_MOTIVO_GLOSA'

query_mot_dev = 'SELECT CD_MOT_DEV, DS_MOT_DEV FROM DBAMV.VW_EXIMIO_MOT_DEV'