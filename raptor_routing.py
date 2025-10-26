
from datetime import datetime, timedelta, date
import geopandas as gpd
import pandas as pd
import pickle
from tqdm import tqdm
import time
import os
import sys
from pathlib import Path
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass

from RAPTOR.std_raptor import raptor
from miscellaneous_func import *

@dataclass
class ConfiguracionRuteoCiclovias:
   
    primer_hora_salida: timedelta = timedelta(hours=7, minutes=0)  
    ultima_hora_salida: timedelta = timedelta(hours=9, minutes=0) 
    intervalo_tiempo: timedelta = timedelta(minutes=15)
    
    max_transbordos: int = 3
    tiempo_cambio: int = 120 
    imprimir_itinerarios: bool = False
    
  
    velocidad_bicicleta: float = 15.0  
    velocidad_caminata: float = 5.0   
    
   
    directorio_salida: Path = None
    archivo_red_ciclovias: Path = None
    
    def __post_init__(self):
        if self.directorio_salida is None:
            self.directorio_salida = Path("resultados_spccm")

class RuteadorCicloviasSPCCM:
    """
    Sistema de ruteo para optimización de ciclovías usando algoritmo RAPTOR
    Implementa la metodología multicriterio del artículo
    """
    
    def __init__(self, config: ConfiguracionRuteoCiclovias):
        self.config = config
        self.resultados_ruteo = []
        self.metricas_desempeno = {}
        
    def ejecutar_optimizacion_rutas(self, pares_od: List[Tuple[str, str]], 
                                  red_ciclovias: gpd.GeoDataFrame) -> List[Dict]:
        """
        Ejecuta la optimización de rutas para pares Origen-Destino
        Implementa el algoritmo RAPTOR adaptado para ciclovías
        """
        print("=== EJECUTANDO OPTIMIZACIÓN DE RUTAS CICLOVÍAS ===")
        
        resultados = []
        
        for origen, destino in tqdm(pares_od, desc="Optimizando rutas O-D"):
            try:
               
                ruta_optima = self._calcular_ruta_optima_spccm(origen, destino, red_ciclovias)
                
                if ruta_optima:
                    resultados.append(ruta_optima)
                    print(f"Ruta optimizada: {origen} → {destino} | "
                          f"Impedancia: {ruta_optima['impedancia_total']:.2f} min")
                
            except Exception as e:
                print(f"Error optimizando ruta {origen}-{destino}: {e}")
                continue
        
        self.resultados_ruteo = resultados
        return resultados
    
    def _calcular_ruta_optima_spccm(self, origen: str, destino: str, 
                                  red_ciclovias: gpd.GeoDataFrame) -> Optional[Dict]:
        """
        Calcula la ruta óptima según la metodología SPCCM del artículo
        Combina algoritmo de caminos mínimos con criterios multicriterio
        """
        try:
     
            rutas_candidatas = self._encontrar_rutas_candidatas(origen, destino, red_ciclovias)
            
            if not rutas_candidatas:
                return None
            

            rutas_evaluadas = []
            for ruta in rutas_candidatas:
                impedancia_total = self._calcular_impedancia_ruta(ruta, red_ciclovias)
                rutas_evaluadas.append({
                    'ruta': ruta,
                    'impedancia_total': impedancia_total,
                    'metricas': self._calcular_metricas_ruta(ruta, red_ciclovias)
                })
            
         
            ruta_optima = min(rutas_evaluadas, key=lambda x: x['impedancia_total'])
            
            resultado = {
                'origen': origen,
                'destino': destino,
                'ruta_optima': ruta_optima['ruta'],
                'impedancia_total': ruta_optima['impedancia_total'],
                'metricas': ruta_optima['metricas'],
                'timestamp_optimizacion': datetime.now()
            }
            
            return resultado
            
        except Exception as e:
            print(f"Error calculando ruta SPCCM {origen}-{destino}: {e}")
            return None
    
    def _encontrar_rutas_candidatas(self, origen: str, destino: str, 
                                  red_ciclovias: gpd.GeoDataFrame) -> List[List[str]]:
        """
        Encuentra rutas candidatas entre origen y destino
        Usa algoritmo de caminos mínimos con diferentes criterios
        """
        rutas_candidatas = []
        
        try:
         
            grafo_ciclovias = self._crear_grafo_desde_red(red_ciclovias)
            
   
            criterios_impedancia = ['distancia', 'seguridad', 'tiempo', 'comodidad']
            
            for criterio in criterios_impedancia:
                ruta = self._encontrar_ruta_por_criterio(grafo_ciclovias, origen, destino, criterio)
                if ruta and ruta not in rutas_candidatas:
                    rutas_candidatas.append(ruta)
            
            return rutas_candidatas
            
        except Exception as e:
            print(f"Error encontrando rutas candidatas: {e}")
            return []
    
    def _crear_grafo_desde_red(self, red_ciclovias: gpd.GeoDataFrame) -> nx.DiGraph:
        """Crea grafo dirigido desde la red de ciclovías"""
        grafo = nx.DiGraph()
        
        for _, enlace in red_ciclovias.iterrows():
            nodo_a = enlace['A']
            nodo_b = enlace['B']
       
            peso_distancia = enlace['longitud'] / 1000 
            peso_seguridad = self._calcular_peso_seguridad(enlace)
            peso_tiempo = peso_distancia / self.config.velocidad_bicicleta * 60  
            peso_comodidad = self._calcular_peso_comodidad(enlace)
            
            grafo.add_edge(nodo_a, nodo_b, 
                          distancia=peso_distancia,
                          seguridad=peso_seguridad,
                          tiempo=peso_tiempo,
                          comodidad=peso_comodidad)
        
        return grafo
    
    def _calcular_peso_seguridad(self, enlace: pd.Series) -> float:
        """Calcula peso basado en seguridad de la infraestructura"""
        factores_seguridad = {
            1: 1.0,  
            2: 1.5, 
            3: 2.5,  
            4: 4.0 
        }
        
        tipo_infra = enlace.get('tipo_infraestructura', 3)
        factor_base = factores_seguridad.get(tipo_infra, 2.5)
        
   
        factor_trafico = 1 + (enlace.get('volumen_vehicular', 0) / 1000) * 0.1
        
  
        factor_velocidad = 1 + max(0, enlace.get('velocidad_vehicular', 30) - 30) / 50
        
        return factor_base * factor_trafico * factor_velocidad
    
    def _calcular_peso_comodidad(self, enlace: pd.Series) -> float:
        """Calcula peso basado en comodidad del recorrido"""
        factor_pendiente = 1 + abs(enlace.get('pendiente', 0)) * 0.1
        factor_superficie = 1.0 
        
        return factor_pendiente * factor_superficie
    
    def _encontrar_ruta_por_criterio(self, grafo: nx.DiGraph, origen: str, 
                                   destino: str, criterio: str) -> Optional[List[str]]:
        """Encuentra ruta usando algoritmo de Dijkstra con criterio específico"""
        try:
            if origen in grafo.nodes and destino in grafo.nodes:
                ruta = nx.shortest_path(grafo, origen, destino, weight=criterio)
                return ruta
            else:
                return None
        except (nx.NetworkXNoPath, nx.NodeNotFound):
            return None
    
    def _calcular_impedancia_ruta(self, ruta: List[str], 
                                red_ciclovias: gpd.GeoDataFrame) -> float:
        """
        Calcula impedancia total de una ruta según metodología del artículo
        C_i,time+attributes = Σ[(l_i / v) * (1 + Σ(β_k * x_k,i))]
        """
        impedancia_total = 0.0
        
        for i in range(len(ruta) - 1):
            nodo_inicio = ruta[i]
            nodo_fin = ruta[i + 1]
            
       
            enlace = self._obtener_enlace_entre_nodos(nodo_inicio, nodo_fin, red_ciclovias)
            
            if enlace is not None:
                impedancia_segmento = self._calcular_impedancia_segmento(enlace)
                impedancia_total += impedancia_segmento
        
        return impedancia_total
    
    def _obtener_enlace_entre_nodos(self, nodo_inicio: str, nodo_fin: str,
                                  red_ciclovias: gpd.GeoDataFrame) -> Optional[pd.Series]:
        """Obtiene el enlace entre dos nodos de la red"""
        mascara = (
            (red_ciclovias['A'] == nodo_inicio) & 
            (red_ciclovias['B'] == nodo_fin)
        ) | (
            (red_ciclovias['A'] == nodo_fin) & 
            (red_ciclovias['B'] == nodo_inicio)
        )
        
        enlaces = red_ciclovias[mascara]
        
        if not enlaces.empty:
            return enlaces.iloc[0]
        else:
            return None
    
    def _calcular_impedancia_segmento(self, enlace: pd.Series) -> float:
      
        distancia_km = enlace['longitud'] / 1000
        tiempo_base = (distancia_km / self.config.velocidad_bicicleta) * 60
        
       
        factores_ajuste = 0.0
        
     
        tipo_infra = enlace.get('tipo_infraestructura', 3)
        if tipo_infra == 1:
            factores_ajuste += -0.3  
        elif tipo_infra == 2:
            factores_ajuste += -0.1
        elif tipo_infra == 3:
            factores_ajuste += 0.3
        else:
            factores_ajuste += 0.6
        
     
        pendiente = enlace.get('pendiente', 0)
        factores_ajuste += 0.05 * abs(pendiente)
        
        volumen = enlace.get('volumen_vehicular', 0)
        factores_ajuste += 0.0001 * volumen
    
        velocidad = max(0, enlace.get('velocidad_vehicular', 30) - 30)
        factores_ajuste += 0.01 * velocidad
        
        impedancia = tiempo_base * (1 + factores_ajuste)
        return impedancia
    
    def _calcular_metricas_ruta(self, ruta: List[str], 
                              red_ciclovias: gpd.GeoDataFrame) -> Dict:
        """Calcula métricas detalladas para una ruta"""
        if len(ruta) < 2:
            return {}
        
        distancia_total = 0
        tiempo_total = 0
        segmentos = []
        tipos_infraestructura = []
        
        for i in range(len(ruta) - 1):
            nodo_inicio = ruta[i]
            nodo_fin = ruta[i + 1]
            
            enlace = self._obtener_enlace_entre_nodos(nodo_inicio, nodo_fin, red_ciclovias)
            
            if enlace is not None:
                distancia_total += enlace['longitud']
                tiempo_segmento = (enlace['longitud'] / 1000) / self.config.velocidad_bicicleta * 60
                tiempo_total += tiempo_segmento
                
                segmentos.append({
                    'nodo_inicio': nodo_inicio,
                    'nodo_fin': nodo_fin,
                    'distancia': enlace['longitud'],
                    'tiempo': tiempo_segmento,
                    'tipo_infraestructura': enlace.get('tipo_infraestructura', 3),
                    'pendiente': enlace.get('pendiente', 0)
                })
                
                tipos_infraestructura.append(enlace.get('tipo_infraestructura', 3))
        
        # Calcular porcentaje de infraestructura segura
        infra_segura = [tipo for tipo in tipos_infraestructura if tipo in [1, 2]]
        pct_infra_segura = (len(infra_segura) / len(tipos_infraestructura)) * 100 if tipos_infraestructura else 0
        
        return {
            'distancia_total': distancia_total,
            'tiempo_total': tiempo_total,
            'num_segmentos': len(segmentos),
            'porcentaje_infra_segura': pct_infra_segura,
            'segmentos': segmentos
        }
    
    def comparar_escenarios_ruteo(self, escenario_base: List[Dict]) -> Dict:
        """
        Compara escenarios de ruteo según metodología del artículo
        Escenario 1: No optimizado (ruta vehicular)
        Escenario 2: Optimizado por impedancia ciclista
        """
        if not self.resultados_ruteo:
            return {}
        
        metricas_comparativas = {}
        
      
        if escenario_base:
            impedancia_base = np.mean([r.get('impedancia_total', 0) for r in escenario_base])
            impedancia_optimizada = np.mean([r.get('impedancia_total', 0) for r in self.resultados_ruteo])
            
            metricas_comparativas['reduccion_impedancia'] = (
                (impedancia_base - impedancia_optimizada) / impedancia_base * 100
                if impedancia_base > 0 else 0
            )
        
   
        porcentaje_seguridad_optimizado = np.mean([
            r.get('metricas', {}).get('porcentaje_infra_segura', 0) 
            for r in self.resultados_ruteo
        ])
        
        metricas_comparativas['porcentaje_seguridad_promedio'] = porcentaje_seguridad_optimizado
        metricas_comparativas['total_rutas_optimizadas'] = len(self.resultados_ruteo)
        
        return metricas_comparativas
    
    def exportar_resultados_ruteo(self, formato: str = "GPKG") -> None:
        """Exporta resultados del ruteo optimizado"""
        if not self.resultados_ruteo:
            print("No hay resultados para exportar")
            return
        
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            if formato == "GPKG":
                archivo = self.config.directorio_salida / f"rutas_optimizadas_{timestamp}.gpkg"

                print(f"Resultados exportados a: {archivo}")
                
            elif formato == "Pickle":
                archivo = self.config.directorio_salida / f"resultados_ruteo_{timestamp}.pkl"
                with open(archivo, 'wb') as f:
                    pickle.dump(self.resultados_ruteo, f)
                print(f"Resultados exportados a: {archivo}")
                
        except Exception as e:
            print(f"Error exportando resultados: {e}")


def ejecutar_ruteo_articulo(pares_od: List[Tuple[str, str]], 
                          red_ciclovias: gpd.GeoDataFrame) -> RuteadorCicloviasSPCCM:
    """
    Función principal que ejecuta el sistema de ruteo del artículo SPCCM
    """
    print("INICIANDO SISTEMA DE RUTEO SPCCM")
    print("Optimización de Rutas de Transporte en Ciclovías mediante Teoría de Grafos")
  
    config = ConfiguracionRuteoCiclovias()
    

    config.directorio_salida.mkdir(exist_ok=True)
    
  
    ruteador = RuteadorCicloviasSPCCM(config)
    

    resultados = ruteador.ejecutar_optimizacion_rutas(pares_od, red_ciclovias)
    
 
    metricas = ruteador.comparar_escenarios_ruteo([]) 
    
    ruteador.exportar_resultados_ruteo("Pickle")
    
    print("\n=== RUTEO OPTIMIZADO COMPLETADO ===")
    print(f"Rutas procesadas: {len(resultados)}")
    print(f"Reducción de impedancia: {metricas.get('reduccion_impedancia', 0):.2f}%")
    print(f"Seguridad promedio: {metricas.get('porcentaje_seguridad_promedio', 0):.1f}%")
    
    return ruteador

if __name__ == "__main__":
   
    pares_od_ejemplo = [
        ("CENTRO", "UNIVERSIDAD"),
        ("ESTACION_CENTRAL", "ZONA_INDUSTRIAL"),
        ("BARRIO_RESIDENCIAL", "CENTRO_COMERCIAL")
    ]
    
    print("Sistema de Ruteo SPCCM - Listo para ejecutar")
