from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, SmallInteger, BigInteger, ForeignKey, Enum, Date, Boolean
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base

import pandas as pd
import json

if __name__ == '__main__':

    # Read config file
    with open('config.json') as config_file:
        config = json.load(config_file)

    db_uri = config['DB_URI']
    file_path = config['FILE_PATH']
    
    # Establish connection
    engine = create_engine(db_uri)
    connection = engine.connect()

    # Declare base
    Base = declarative_base()

    # Read csv file and create dataframe
    df = pd.read_csv(file_path, encoding='latin-1', sep=';')

    # Define a dictionary for replacement
    replacement_dict = {"#NULO#": None, -1: None}

    # Replace null values in the entire DataFrame
    df.replace(replacement_dict, inplace=True)

    # Define columns categories
    siglas_uf_enum = (
        'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MT', 'MS', 
        'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 
        'SP', 'SE', 'TO', 'ZZ', 'BR', 'VT'
    )

    agremiacao_enum = (
        'Federação', 'Coligação', 'Partido isolado', 'Partido', 'Partido em coligação'
    )

    tipo_eleicao_enum = (
        'Eleição Ordinária', 'Eleição Suplementar', 'Consulta Popular', 'Eleição Majoritária'
    )

    abrangencia_enum = (
        'E', 'M', 'F'
    )

    turno_enum = (
        '1', '2'
    )

    # Define database tables
    class Municipios(Base):
        __tablename__ = 'municipios'
        id = Column(Integer, primary_key=True)
        nome = Column(String, nullable=False)

    class Locais(Base):
        __tablename__ = 'locais'
        id_local = Column(SmallInteger, primary_key=True, autoincrement=True)
        id_zona_eleitoral = Column(String(length=5), nullable=False)
        unidade_federacao = Column(Enum(*siglas_uf_enum, name='estados_enum'), nullable=False)
        id_municipio = Column(Integer, ForeignKey('municipios.id'), nullable=False)    

    class Partidos(Base):
        __tablename__ = 'partidos'
        sigla = Column(String(length=10), primary_key=True)
        nome = Column(String, nullable=False)
        numero = Column(SmallInteger, nullable=False)
        
    class Cargo(Base):
        __tablename__ = 'cargo'
        id = Column(SmallInteger, primary_key=True)
        cargo = Column(String, nullable=False)

    class Situacao(Base):
        __tablename__ = 'situacao'
        id = Column(SmallInteger, primary_key=True)
        situacao = Column(String, nullable=False)

    class Federacao(Base):
        __tablename__ = 'federacao'
        numero = Column(Integer, primary_key=True)
        nome = Column(String, nullable=False)
        sigla = Column(String(length=10))
        composicao = Column(String)

    class Coligacao(Base):
        __tablename__ = 'coligacao'
        numero = Column(BigInteger, primary_key=True)
        nome = Column(String, nullable=False)
        composicao = Column(String, nullable=False)

    class SituacaoDetalhe(Base):
        __tablename__ = 'situacao_detalhe'
        id = Column(SmallInteger, primary_key=True)
        descricao = Column(String(length=50), nullable=False)

    class Candidato(Base):
        __tablename__ = 'candidato'
        id = Column(BigInteger, primary_key=True)
        nome = Column(String, nullable=False)
        nome_urna = Column(String, nullable=False)
        nome_social = Column(String)
        numero_candidatura = Column(SmallInteger, nullable=False)
        unidade_eleitoral_sigla = Column(String(length=50))
        unidade_eleitoral = Column(String)
        tipo_agremiacao = Column(Enum(*agremiacao_enum, name='agremiacao_enum'))
        cargo = Column(SmallInteger, ForeignKey('cargo.id'), nullable=False)
        situacao = Column(SmallInteger, ForeignKey('situacao.id'))
        partido = Column(String(length=10), ForeignKey('partidos.sigla'), nullable=False)
        federacao = Column(Integer, ForeignKey('federacao.numero'))
        coligacao = Column(BigInteger, ForeignKey('coligacao.numero'))
        situacao_detalhe = Column(SmallInteger, ForeignKey('situacao_detalhe.id'))

    class Eleicao(Base):
        __tablename__ = 'eleicao'
        id = Column(Integer, primary_key=True)
        tipo = Column(Enum(*tipo_eleicao_enum, name='tipo_eleicao_enum'), nullable=False)
        turno = Column(SmallInteger, nullable=False)
        descricao = Column(String)
        data = Column(Date, nullable=False)
        abrangencia = Column(Enum(*abrangencia_enum, name='abrangencia_enum'))

    class SituacaoTotalizacao(Base):
        __tablename__ = 'situacao_totalizacao'
        id = Column(SmallInteger, primary_key=True)
        descricao = Column(String(length=50), nullable=False)

    class QtdVotos(Base):
        __tablename__ = 'qtd_votos'
        id = Column(Integer, primary_key=True, autoincrement=True)
        eleicao = Column(Integer, ForeignKey('eleicao.id'), nullable=False)
        candidato = Column(BigInteger, ForeignKey('candidato.id'), nullable=False)
        turno = Column(Enum(*turno_enum, name='turno_enum'), nullable=False)
        local = Column(Integer, ForeignKey('locais.id_local'), nullable=False)
        qtd_votos = Column(Integer, nullable=False)
        voto_em_transito = Column(Boolean)
        tipo_destinacao = Column(String(length=50))
        qtd_votos_validos = Column(Integer)
        situacao_tot = Column(SmallInteger, ForeignKey('situacao_totalizacao.id'))    
        
    # Create the tables in the database
    Base.metadata.create_all(engine)

    # Create a session
    Session = sessionmaker(bind = engine)
    session = Session()

    # Insert data to tables
    try:
        checkpoint = session.begin_nested()
        for idx, row in df.drop_duplicates('CD_MUNICIPIO').iterrows():
            entry_municipios = Municipios(
                id=row['CD_MUNICIPIO'], 
                nome=row['NM_MUNICIPIO']
                )
            session.add(entry_municipios)
    except:
        print("An exception occurred while inserting data to a table. Rolling back...")
        session.rollback()
        session.close()
        print("Rollback completed.")
        raise
    else:
        session.commit()

    try:
        checkpoint = session.begin_nested()
        for idx, row in df.drop_duplicates(subset=['NR_ZONA', 'CD_MUNICIPIO']).iterrows():
            entry_locais = Locais(
                id_zona_eleitoral=row['NR_ZONA'], 
                unidade_federacao=row['SG_UF'], 
                id_municipio=row['CD_MUNICIPIO']
                )
            session.add(entry_locais)
    except:
        print("An exception occurred while inserting data to a table. Rolling back...")
        checkpoint.rollback()
        session.close()
        print("Rollback completed.")
        raise
    else:
        session.commit()

    try:
        checkpoint = session.begin_nested()
        for idx, row in df.drop_duplicates('SG_PARTIDO').iterrows():
            entry_partidos = Partidos(
                sigla=row['SG_PARTIDO'], 
                nome=row['NM_PARTIDO'],
                numero=row['NR_PARTIDO']
                )
            session.add(entry_partidos)
    except:
        print("An exception occurred while inserting data to a table. Rolling back...")
        session.rollback()
        session.close()
        print("Rollback completed.")
        raise
    else:
        session.commit()

    try:
        checkpoint = session.begin_nested()
        for idx, row in df.drop_duplicates('CD_CARGO').iterrows():
            entry_cargo = Cargo(
                id=row['CD_CARGO'], 
                cargo=row['DS_CARGO']
                )
            session.add(entry_cargo)
    except:
        print("An exception occurred while inserting data to a table. Rolling back...")
        session.rollback()
        session.close()
        print("Rollback completed.")
        raise
    else:
        session.commit()

    try:
        checkpoint = session.begin_nested()
        for idx, row in df.drop_duplicates('CD_SITUACAO_CANDIDATURA').iterrows():
            entry_situacao = Situacao(
                id=row['CD_SITUACAO_CANDIDATURA'], 
                situacao=row['DS_SITUACAO_CANDIDATURA']
                )
            session.add(entry_situacao)
    except:
        print("An exception occurred while inserting data to a table. Rolling back...")
        session.rollback()
        session.close()
        print("Rollback completed.")
        raise
    else:
        session.commit()

    try:
        checkpoint = session.begin_nested()
        for idx, row in df.drop_duplicates(subset=['NR_FEDERACAO', 'DS_COMPOSICAO_FEDERACAO']).iterrows():
            entry_federacao = Federacao(
                numero=row['NR_FEDERACAO'], 
                nome=row['NM_FEDERACAO'],
                sigla=row['SG_FEDERACAO'],
                composicao=row['DS_COMPOSICAO_FEDERACAO']
                )
            session.add(entry_federacao)
    except:
        print("An exception occurred while inserting data to a table. Rolling back...")
        session.rollback()
        session.close()
        print("Rollback completed.")
        raise
    else:
        session.commit()

    try:
        checkpoint = session.begin_nested()
        for idx, row in df.drop_duplicates('SQ_COLIGACAO').iterrows():
            entry_coligacao = Coligacao(
                numero=row['SQ_COLIGACAO'], 
                nome=row['NM_COLIGACAO'],
                composicao=row['DS_COMPOSICAO_COLIGACAO']
                )
            session.add(entry_coligacao)
    except:
        print("An exception occurred while inserting data to a table. Rolling back...")
        session.rollback()
        session.close()
        print("Rollback completed.")
        raise
    else:
        session.commit()

    try:
        checkpoint = session.begin_nested()
        for idx, row in df.drop_duplicates('CD_DETALHE_SITUACAO_CAND').iterrows():
            entry_situacao_detalhe = SituacaoDetalhe(
                id=row['CD_DETALHE_SITUACAO_CAND'], 
                descricao=row['DS_DETALHE_SITUACAO_CAND']
                )
            session.add(entry_situacao_detalhe)
    except:
        print("An exception occurred while inserting data to a table. Rolling back...")
        session.rollback()
        session.close()
        print("Rollback completed.")
        raise
    else:
        session.commit()

    try:
        checkpoint = session.begin_nested()
        for idx, row in df.drop_duplicates('SQ_CANDIDATO').iterrows():
            entry_candidato = Candidato(
                id=row['SQ_CANDIDATO'], 
                nome=row['NM_CANDIDATO'],
                nome_urna=row['NM_URNA_CANDIDATO'],
                nome_social=row['NM_SOCIAL_CANDIDATO'],
                numero_candidatura=row['NR_CANDIDATO'],
                unidade_eleitoral_sigla=row['SG_UE'],
                unidade_eleitoral=row['NM_UE'],
                tipo_agremiacao=row['TP_AGREMIACAO'],
                cargo=row['CD_CARGO'],
                situacao=row['CD_SITUACAO_CANDIDATURA'],
                partido=row['SG_PARTIDO'],
                federacao=row['NR_FEDERACAO'],
                coligacao=row['SQ_COLIGACAO'],
                situacao_detalhe=row['CD_DETALHE_SITUACAO_CAND']
                )
            session.add(entry_candidato)
    except:
        print("An exception occurred while inserting data to a table. Rolling back...")
        session.rollback()
        session.close()
        print("Rollback completed.")
        raise
    else:
        session.commit()

    try:
        checkpoint = session.begin_nested()
        for idx, row in df.drop_duplicates('CD_ELEICAO').iterrows():
            entry_eleicao = Eleicao(
                id=row['CD_ELEICAO'],
                tipo=row['NM_TIPO_ELEICAO'],
                turno=row['NR_TURNO'],
                descricao=row['DS_ELEICAO'],
                data=pd.to_datetime(row['DT_ELEICAO'], format='%d/%m/%Y').date(),
                abrangencia=row['TP_ABRANGENCIA']
                )
            session.add(entry_eleicao)
    except:
        print("An exception occurred while inserting data to a table. Rolling back...")
        session.rollback()
        session.close()
        print("Rollback completed.")
        raise
    else:
        session.commit()

    try:
        checkpoint = session.begin_nested()
        for idx, row in df.drop_duplicates('CD_SIT_TOT_TURNO').iterrows():
            entry_situacao_tot = SituacaoTotalizacao(
                id=row['CD_SIT_TOT_TURNO'], 
                descricao=row['DS_SIT_TOT_TURNO']
                )
            session.add(entry_situacao_tot)
    except:
        print("An exception occurred while inserting data to a table. Rolling back...")
        session.rollback()
        session.close()
        print("Rollback completed.")
        raise
    else:
        session.commit()

    try:
        checkpoint = session.begin_nested()
        for idx, row in df.iterrows():
            entry_qtd_votos = QtdVotos(
                eleicao=row['CD_ELEICAO'],
                candidato=row['SQ_CANDIDATO'],
                turno=str(row['NR_TURNO']),
                local=row['NR_ZONA'],
                qtd_votos=row['QT_VOTOS_NOMINAIS'],
                voto_em_transito=True if row['ST_VOTO_EM_TRANSITO'] == 'S' else False,
                tipo_destinacao=row['NM_TIPO_DESTINACAO_VOTOS'],
                qtd_votos_validos=row['QT_VOTOS_NOMINAIS_VALIDOS'],
                situacao_tot=row['CD_SIT_TOT_TURNO']
                )
            session.add(entry_qtd_votos)
    except:
        print("An exception occurred while inserting data to a table. Rolling back...")
        session.rollback()
        session.close()
        print("Rollback completed.")
        raise
    else:
        session.commit()

    # Close the session
    session.close()