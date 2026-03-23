# Workflow definition + DAG builder

from __future__ import annotations
from collections import defaultdict,deque
from dataclasses import dataclass,field
from typing import Any,Callable

from vortex.agent import Agent

@dataclass
class ConditionalEdge:
    source:str
    true_target:str
    false_target:str
    condition:Callable[[dict[str,Any]],bool]

class Workflow:
    def __init__(
        self,
        name:str,
        agents:list[Agent],
        edges:list[tuple[str,str]] | None=None,
        conditional_edges: list[ConditionalEdge] | None=None,

    ):

        self.name=name
        self.agents={agent.name:agent for agent in agents}
        self.edges=edges or []
        self.conditional_edges= conditional_edges or []
        self._adj: dict[str,list[str]]=defaultdict(list)
        self._in_degree:dict[str,int]={}


        self._build_graph()
        self._validate()

    def _build_graph(self):
        for name in self.agents:
            self._in_degree[name]=0

        for src,dst in self.edges:
            self._adj[src].append(dst)
            self._in_degree[dst]=self._in_degree.get(dst,0)+1

        #ce is conditional edge
        for ce in self.conditional_edges:
            self._adj[ce.source].append(ce.true_target)
            self._adj[ce.source].append(ce.false_target)
            self._in_degree[ce.true_target] = self._in_degree.get(ce.true_target, 0) + 1
            self._in_degree[ce.false_target] = self._in_degree.get(ce.false_target, 0) + 1


    def _validate(self):
        for src,dst in self.edges:
            if src not in self.agents:
                raise ValueError(f"Unknown agent in edge: {src!r}")
            if dst not in self.agents:
                raise ValueError(f"Unknown agent in edge: {dst!r}")



        if self.has_cycle():
            raise ValueError(f"Workflow {self.name!r} has a cycle - must be a DAG")


    #Kahn's algorithm hai ye basically topological sort karke cycle detect karta hai
    def has_cycle(self)->bool:
        visited=set()
        count=0
        queue=deque(
            name for name,deg in self._in_degree.items() if deg==0
        )

        temp_in=dict(self._in_degree)

        while queue:
            node=queue.popleft()
            visited.add(node)
            count+=1
            for neighbor in self._adj[node]:
                 temp_in[neighbor]-=1
                 if temp_in[neighbor]==0:
                    queue.append(neighbor)

        return count!=len(self.agents)


    def topological_order(self)->list[str]:
        """Returns agent grouped by level - each level can run in parallel"""

        in_deg=dict(self._in_degree)
        queue=deque(n for n,d in in_deg.items() if d==0)
        levels=[]


        while queue:
            level=list(queue)
            levels.append(level)
            queue=deque()
            for node in level:
                for neighbor in self._adj[node]:
                    in_deg[neighbor]-=1
                    if in_deg[neighbor]==0:

                        queue.append(neighbor)

        return levels


    def get_conditional_edge(self,source:str)->ConditionalEdge | None:
        for ce in self.conditional_edges:
            if ce.source==source:
                return ce
        return None

    def get_conditional_edge_for_target(self, target: str) -> ConditionalEdge | None:
       for ce in self.conditional_edges:
          if ce.true_target == target or ce.false_target == target:
               return ce
       return None
        