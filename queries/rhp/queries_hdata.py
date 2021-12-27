query_atendime_hdata = 'SELECT A.CD_MULTI_EMPRESA, CD_PACIENTE, CD_ATENDIMENTO, CD_CID, CD_MOT_ALT, CD_TIP_RES, CD_CONVENIO, CD_ESPECIALID, CD_PRESTADOR, CD_ATENDIMENTO_PAI, CD_LEITO, A.CD_ORI_ATE, CD_SERVICO, TP_ATENDIMENTO, DT_ATENDIMENTO, HR_ATENDIMENTO, HR_ALTA, HR_ALTA_MEDICA, CD_TIP_MAR, CD_SINTOMA_AVALIACAO, NM_USUARIO_ALTA_MEDICA FROM MV_RHP.ATENDIME A'

query_cid_hdata = 'SELECT CD_CID, DS_CID, CD_SGRU_CID FROM MV_RHP.CID'

query_classificacao_risco_hdata = 'SELECT CD_CLASSIFICACAO_RISCO, CD_COR_REFERENCIA, CD_TRIAGEM_ATENDIMENTO, DH_CLASSIFICACAO_RISCO FROM MV_RHP.SACR_CLASS_RISCO'

query_classificacao_hdata = 'SELECT CD_CLASSIFICACAO, DS_TIPO_RISCO FROM MV_RHP.SACR_CLASSIFICACAO'

query_convenio_hdata = 'SELECT CD_CONVENIO, NM_CONVENIO FROM MV_RHP.CONVENIO'

query_cor_referencia_hdata = 'SELECT CD_COR_REFERENCIA FROM MV_RHP.SACR_COR_REFERENCIA'

query_diagnostico_atendime_hdata = 'SELECT CD_CID, CD_DIAGNOSTICO_ATENDIME, CD_ATENDIMENTO FROM MV_RHP.DIAGNOSTICO_ATENDIME'

query_documento_clinico_hdata = "SELECT CD_OBJETO, cd_atendimento, cd_tipo_documento, tp_status, DH_CRIACAO FROM MV_RHP.PW_DOCUMENTO_CLINICO"

query_esp_med_hdata = 'SELECT CD_ESPECIALID, CD_PRESTADOR, SN_ESPECIAL_PRINCIPAL FROM MV_RHP.ESP_MED'

query_especialidad_hdata = 'SELECT CD_ESPECIALID, DS_ESPECIALID FROM MV_RHP.ESPECIALID'

query_gru_cid_hdata = 'SELECT CD_GRU_CID, DS_GRU_CID FROM MV_RHP.GRU_CID'

query_mot_alt_hdata = 'SELECT CD_MOT_ALT, DS_MOT_ALT, TP_MOT_ALTA FROM MV_RHP.MOT_ALT'

query_multi_empresa_hdata = 'SELECT CD_MULTI_EMPRESA, DS_MULTI_EMPRESA FROM MV_RHP.MULTI_EMPRESAS'

query_ori_ate_hdata = 'SELECT CD_ORI_ATE, DS_ORI_ATE, TP_ORIGEM, CD_SETOR FROM MV_RHP.ORI_ATE'

query_paciente_hdata = 'SELECT DISTINCT P.CD_PACIENTE, DT_NASCIMENTO, TP_SEXO, DT_CADASTRO, NM_BAIRRO FROM MV_RHP.PACIENTE P'

query_pagu_objeto_hdata = 'SELECT CD_OBJETO, TP_OBJETO FROM MV_RHP.PAGU_OBJETO'

query_registro_alta_hdata = 'SELECT HR_ALTA_MEDICA, CD_ATENDIMENTO FROM MV_RHP.PW_REGISTRO_ALTA'

query_setor_hdata = 'SELECT CD_SETOR, NM_SETOR FROM MV_RHP.SETOR'

query_sgru_cid_hdata = 'SELECT CD_SGRU_CID, CD_GRU_CID, DS_SGRU_CID FROM MV_RHP.SGRU_CID'

query_sintoma_avaliacao_hdata = 'SELECT CD_SINTOMA_AVALIACAO, DS_SINTOMA FROM MV_RHP.SACR_SINTOMA_AV'

query_tempo_processo_hdata = 'SELECT DH_PROCESSO,  CD_TIPO_TEMPO_PROCESSO,  CD_ATENDIMENTO FROM MV_RHP.SACR_TEMPO_PROCESSO'

query_tip_mar_hdata = 'SELECT CD_TIP_MAR FROM MV_RHP.TIP_MAR'

query_tip_res_hdata = 'SELECT CD_TIP_RES, DS_TIP_RES, SN_OBITO FROM MV_RHP.TIP_RES'

query_triagem_atendimento_hdata = 'SELECT CD_ATENDIMENTO, CD_TRIAGEM_ATENDIMENTO, CD_SINTOMA_AVALIACAO, DS_SENHA, DH_PRE_ATENDIMENTO FROM MV_RHP.TRIAGEM_ATENDIMENTO'

query_usuario_hdata = 'SELECT CD_USUARIO, NM_USUARIO FROM MV_RHP.USUARIOS'

query_pre_med_hdata = 'SELECT CD_PRE_MED, PM.CD_ATENDIMENTO, PM.CD_PRESTADOR, CD_DOCUMENTO_CLINICO, DT_PRE_MED, TP_PRE_MED, PM.CD_SETOR FROM MV_RHP.PRE_MED PM'

query_itpre_med_hdata = 'SELECT IP.CD_PRE_MED, CD_ITPRE_MED, CD_PRODUTO, CD_TIP_PRESC, CD_TIP_ESQ, CD_FOR_APL, CD_TIP_FRE, TP_JUSTIFICATIVA FROM MV_RHP.ITPRE_MED IP'

query_tip_presc_hdata = 'SELECT TP.CD_TIP_PRESC, DS_TIP_PRESC, CD_PRO_FAT FROM MV_RHP.TIP_PRESC TP'

query_for_apl_hdata = 'SELECT CD_FOR_APL, DS_FOR_APL FROM MV_RHP.FOR_APL'

query_tip_esq_hdata = 'SELECT CD_TIP_ESQ, DS_TIP_ESQ FROM MV_RHP.TIP_ESQ'

query_tip_fre_hdata = 'SELECT CD_TIP_FRE, DS_TIP_FRE FROM MV_RHP.TIP_FRE'

query_de_para_tuss_hdata = 'SELECT CNHPM_5, CBHPM_4, CNHPM_3, AMB_1990, AMB_1992, AMB_1996, AMB_1999, TUSS FROM MV_RHP.DE_PARA_TUSS'

query_gru_fat_hdata = 'SELECT CD_GRU_FAT, DS_GRU_FAT, TP_GRU_FAT FROM MV_RHP.GRU_FAT'

query_gru_pro_hdata = 'SELECT CD_GRU_PRO, CD_GRU_FAT, DS_GRU_PRO, TP_GRU_PRO, TP_CUSTO FROM MV_RHP.GRU_PRO'

query_produto_hdata = 'SELECT CD_PRO_FAT, DS_PRODUTO, CD_PRODUTO, VL_FATOR_PRO_FAT, SN_OPME, CD_ESPECIE, VL_CUSTO_MEDIO FROM MV_RHP.PRODUTO'

query_pro_fat_hdata = 'SELECT CD_GRU_PRO, CD_PRO_FAT, CD_POR_ANE, DS_PRO_FAT FROM MV_RHP.PRO_FAT'

query_tuss_hdata = 'SELECT CD_PRO_FAT, CD_TUSS, DS_TUSS FROM MV_RHP.TUSS'

query_uni_pro_hdata = 'SELECT CD_UNIDADE, DS_UNIDADE, VL_FATOR, TP_RELATORIOS, CD_UNI_PRO, CD_PRODUTO, SN_ATIVO FROM MV_RHP.UNI_PRO'

query_reg_amb_hdata = 'SELECT DISTINCT RA.CD_REG_AMB, CD_REMESSA, RA.VL_TOTAL_CONTA FROM MV_RHP.REG_AMB RA'

query_itreg_amb_hdata = 'SELECT IRA.CD_ATENDIMENTO, CD_PRO_FAT, CD_REG_AMB, CD_GRU_FAT, CD_LANCAMENTO, QT_LANCAMENTO, VL_UNITARIO, VL_NOTA, IRA.CD_SETOR, CD_SETOR_PRODUZIU, TP_PAGAMENTO, SN_PERTENCE_PACOTE, VL_TOTAL_CONTA, SN_FECHADA, DT_FECHAMENTO, CD_ITMVTO FROM MV_RHP.ITREG_AMB IRA'

query_reg_fat_hdata = 'SELECT CD_REG_FAT, SN_FECHADA, DT_INICIO, DT_FINAL, DT_FECHAMENTO, CD_REMESSA, VL_TOTAL_CONTA, RF.CD_ATENDIMENTO FROM MV_RHP.REG_FAT RF'

query_itreg_fat_hdata = 'SELECT IRF.CD_REG_FAT, CD_LANCAMENTO, DT_LANCAMENTO, QT_LANCAMENTO, TP_PAGAMENTO, VL_UNITARIO, VL_NOTA, IRF.CD_CONTA_PAI, CD_PRO_FAT, CD_GRU_FAT, IRF.VL_TOTAL_CONTA, SN_PERTENCE_PACOTE, IRF.CD_SETOR, CD_SETOR_PRODUZIU, CD_ITMVTO FROM MV_RHP.ITREG_FAT IRF'

query_custo_final_hdata = 'SELECT VL_CUSTO_CENCIR, DT_COMPETENCIA FROM MV_RHP.CUSTO_FINAL'

query_mvto_estoque_hdata = 'SELECT CD_MVTO_ESTOQUE, ME.CD_SETOR, ME.CD_ATENDIMENTO, CD_MOT_DEV, ME.CD_MULTI_EMPRESA, DT_MVTO_ESTOQUE FROM MV_RHP.MVTO_ESTOQUE ME'

query_itmvto_estoque_hdata = 'SELECT CD_ITMVTO_ESTOQUE, QT_MOVIMENTACAO, IME.CD_MVTO_ESTOQUE, CD_PRODUTO, IME.CD_UNI_PRO FROM MV_RHP.ITMVTO_ESTOQUE IME'

query_quantidade_diarias_hdata = 'SELECT QD.CD_ATENDIMENTO, VL_DIARIA, QTD_DIARIAS FROM MV_RHP.QUANTIDADE_DIARIAS QD'

query_remessa_fatura_hdata = 'SELECT RF.CD_REMESSA, DT_ABERTURA, RF.DT_FECHAMENTO, DT_ENTREGA_DA_FATURA FROM MV_RHP.REMESSA_FATURA RF'

query_repasse_hdata = 'SELECT DISTINCT R.CD_REPASSE, DT_COMPETENCIA FROM MV_RHP.REPASSE R'

query_it_repasse_hdata = 'SELECT DISTINCT ITR.CD_REG_FAT, CD_LANCAMENTO_FAT, CD_REPASSE FROM MV_RHP.IT_REPASSE ITR'

query_itent_pro_hdata = 'SELECT VL_TOTAL, ITP.CD_ATENDIMENTO, CD_PRODUTO, VL_UNITARIO, DT_GRAVACAO FROM MV_RHP.ITENT_PRO ITP'

query_glosas_hdata = 'SELECT DISTINCT CD_GLOSAS, CD_REG_FAT, G.CD_REG_AMB, G.CD_MOTIVO_GLOSA, VL_GLOSA, CD_LANCAMENTO_FAT, CD_LANCAMENTO_AMB FROM MV_RHP.GLOSAS G'

query_custo_medio_mensal_hdata = 'SELECT VL_CUSTO_MEDIO, DH_CUSTO_MEDIO, CD_PRODUTO, CD_MULTI_EMPRESA FROM MV_RHP.CUSTO_MEDIO_MENSAL'

query_fa_custo_atendimento_hdata = 'SELECT VL_DIARIA, VL_CUSTO_GASES, VL_CUSTO_REPASSE, VL_CUSTO_MEDICAMENTO, VL_PROCEDIMENTO, VL_CUSTO_DIARIATAXA, FCA.CD_ATENDIMENTO FROM MV_RHP.FA_CUSTO_ATENDIMENTO FCA'

query_especie_hdata = 'SELECT CD_ESPECIE, DS_ESPECIE FROM MV_RHP.ESPECIE'

query_exa_lab_hdata = 'SELECT CD_PRO_FAT, CD_EXA_LAB, NM_EXA_LAB FROM MV_RHP.EXA_LAB'

query_exa_rx_hdata = 'SELECT EXA_RX_CD_PRO_FAT, CD_EXA_RX, DS_EXA_RX FROM MV_RHP.EXA_RX'

query_gru_fat_hdata = 'SELECT CD_GRU_FAT, DS_GRU_FAT, TP_GRU_FAT FROM MV_RHP.GRU_FAT'

query_gru_pro_hdata = 'SELECT CD_GRU_PRO, CD_GRU_FAT, DS_GRU_PRO, TP_GRU_PRO, TP_CUSTO FROM MV_RHP.GRU_PRO'

query_motivo_glosa_hdata = 'SELECT DS_MOTIVO_GLOSA, CD_MOTIVO_GLOSA FROM MV_RHP.MOTIVO_GLOSA'

query_mot_dev_hdata = 'SELECT CD_MOT_DEV, DS_MOT_DEV FROM MV_RHP.MOT_DEV'