# NEBULA: NEtworked Blockchain emULAtor

This project aims to provide an extensible, modular and scalable framework for the emulation of network and specifically blockchain architectures. 
Emulated network peers are hosted on coordinator VMs in an n-to-m fashion, allowing for hardware-in-the-loop integration.
By configuring parameters starting with network size and topology, to intricate blockchain strategies, experiments are conducted in an autonomous and repeatable fashion.
During execution, measurements about confirmation latencies, stale blocks and more are made, in order to determine performance and efficiency of the emulated blockchain platform.

---

If you use NEBULA for your research, please cite our paper: 

Leo Eichhorn, Tanya Shreedhar, Aleksandr Zavodovski, and Nitinder Mohan. 2021. Distributed Ledgers for Distributed Edge: Are we there yet?. In
*Interdisciplinary Workshop on (de) Centralization in the Internet (IWCI’21),
December 7, 2021, Virtual Event, Germany.* ACM, New York, NY, USA, 8 pages.
https://doi.org/10.1145/3488663.3493687

---

## Requirements
- Java 11+
- Maven 3.6.2+

## Build

Clean artifacts:

    $mvn clean

Build and run tests (~15 min):

    $mvn package (-DskipTests)

Generate code coverage and bug reports, viewable under `target/site/index.html`:

    $mvn clean install site
	
This generates the following reports:
- [JaCoCo](https://www.eclemma.org/jacoco/trunk/index.html)
- [Checkstyle](https://checkstyle.sourceforge.io/)
- [PMD](https://pmd.github.io/)
- [SpotBugs](https://spotbugs.github.io/)

## Application Configuration
An emulation experiment is defined by constructing a config file in YAML format and sharing it between coordinators. 
Exemplary configurations for common blockchains can be found under `src/config`. 
The config file is composed of multiple parts:

#### Coordinator Definition
Specify a list of addresses hosting individual coordinators along with their computing share. 
Computing share can be 0 and defines the share of total peers to be hosted on each coordinator.
Typically, this is achieved by specifying the number of cores on each coordinator or using the exact number of peers.
Addresses are the same ones used to start individual coordinator jar-files later.
```yaml
coordinators:
  - address: 192.168.0.124:5151
    computingShare: 40
  - address: localhost:5152
    computingShare: 47
  - address: vm.example.com:5153
    computingShare: 39
  - ...
```

#### Blockchain Defaults
Defines blockchain and network strategies to be used in this experiment.
-  ``networkType``, ``blockchainType`` select blockchain and network strategies to be used.
- ``miningRate`` defines number of blocks created per second by the entire network. 
- ``txRate`` defines number of transactions created per second by the entire network.
- ``txSize`` constant size of transaction payload in byte.
- ``txFees`` specify transaction fees and the share of their occurrence. When filling blocks, transactions with higher fees are included first.
- ``txDistribution`` time distribution of transactions created by individual peers.
- ``txPeerDistribution`` distribution of amount of transactions created between all peers (constant = all peers create the same amount of transactions on average).
- ``blockSize`` maximum number of transaction fitting in a single block.
- ``forkResolution`` choose between longest chain rule or GHOST.
- ``tieResolution`` used to choose parent block in case fork resolution results in a tie.
- ``ghostDepth`` depth of the blockchain to backtrack during GHOST fork resolution.
- ``txPoolSize`` maximum number of transactions fitting into pool without being included in the blockchain. Further Transactions are rejected.
- ``verificationTime`` simulated time in microseconds taken to verify received blocks before adding them to the chain.
- ``blocks`` number of blocks to be created until experiment end.
- ``pushBlocks`` true - use unsolicited block push as opposed to advertisement based gossipping.
- ``simulateFullBlocks`` true - add artificial delay when sending blocks as if the block were full.
```yaml
networkType: random # random | scaleFree | explicit

blockchainType: proofBased # proofBased | dPoSBFT

blockchainDefaults:
  miningRate: 10                # double in [0,...]
  txRate: 2                     # double in [0,...]
  txSize: 0                     # integer in [0,...]
  txFees: [{fee: 3, share: 1}]  # list
  txDistribution: poisson       # constant | uniform | poisson
  txPeerDistribution: 
    type: constant              # constant | uniform | exponential
    mean: 1                     # double in (0,...]
  blockSize: 6500               # integer in [0,...]
  forkResolution: LCR           # LCR | GHOST
  tieResolution: random         # random | first
  ghostDepth: 8                 # integer in [1,...]
  txPoolSize: 200000            # integer in [0,...]
  verificationTime: 0           # integer in [0,...]
  blocks: 1000                  # integer in [1,...]
  pushBlocks: true              # bool
  simulateFullBlocks: false     # bool            

```

#### Network Strategy
Define composition and topology of the underlying peer-to-peer network. Three strategies are supported so far:

1. Random Graph Strategy:

    - Creates a random graph with the given number of ``nodes`` and edge ``density``. The resulting graph will always be connected.
    - ``latency`` the emulated latency in microseconds.
    - ``perEdge`` true - ``latency`` is applied to every edge. false - ``latency`` is used as the average propagation delay between any two nodes.
    - ``bandwidth`` emulated bandwidth in MB/s
     
    ```yaml
    random:
      nodes: 500       # integer in [1,...]
      density: 0.01    # double in (0,1]
      latency: 63000   # integer in [0,...]
      perEdge: false   # bool
      bandwidth: 2000  # integer in [1,...]
    ```

2. Scale-Free Graph Strategy:

    - Creates a random scale-free graph according to the Barabási–Albert model with parameters ``nodes`` and ``m``.
    - Remaining parameters as defined above.

    ```yaml
    scaleFree:
      nodes: 100       # integer in [1,...]
      m: 2             # integer in [1,...]
      latency: 63000   # integer in [0,...]
      perEdge: false   # bool
      bandwidth: 2000  # integer in [1,...]
    ```

3. Explicit Graph Strategy:
    
    - Creates a graph according to the explicitly defined adjacency or edge list. The defined graph has to be connected.
    - ``fileName`` file path to the txt-file defining an edge list of form:
        ```txt
        <from_id> <to_id> <latency_in_microseconds>
        <from_id> <to_id> <latency_in_microseconds>
        ...
        ```
    - ``peers`` If no file name is supplied, graph is specified here as an adjacency list by defining peers and their edges. Entries for all ids from 0 to n are required.

    ```yaml
    explicit:
      bandwidth: 2000 # integer in [1,...]
      fileName: cloudGraph.txt
      peers:
        - id: 0
          edges: [{id: 1, latency: 10}, {id: 2, latency: 20}]
        - id: 1
          edges: [{id: 2, latency: 20}] # latency in [0,...] microseconds
        - id: 2
          edges: []
        ...
    ```

#### Blockchain Strategy
Define blockchain strategy to be used as selected in blockchain defaults. Two implementations are supported:  

1. Proof-Based Strategy:
    - ``confirmations`` number of confirmation blocks required until transaction latency is determined.
    - ``miningDistribution`` distribution of blocks created between all peers (constant = all peers create the same amount of blocks on average).
    - ``peers`` list used to override default configuration of ``blockchainDefaults`` for specific peers (may be empty).

    ```yaml
    proofBased:
      confirmations: 1 # integer in [1,...]
      miningDistribution:
        type: constant # constant | uniform | exponential
        mean: 1 # ignored for type: uniform
      peers: 
      - id: 0
        miningShare: 200 # default: 1
        txShare: 1 # default: 1
        txSize: 100
        txFees: [{fee: 3, share: 1}]
        txPoolSize: 1000
        verificationTime: 5
        txDistribution: uniform # constant | uniform | poisson
      - id: 1
        miningShare: 1
        txShare: 1
        txSize: 101
        txFees: [{fee: 5, share: 1}]
        txPoolSize: 1001
        verificationTime: 5000
      ...
    ```

2. DPoS-BFT Strategy:
    - ``consensusNodes`` explicitly define ids to be selected as block producers.
    - ``consensusNodeNum`` if ``consensusNodes`` is undefined, select the specified number of nodes as block producers automatically.
    - ``nodeSelection`` block producers can be selected based on their network degree using strategies best, random or worst.
    - ``blocksPerNode`` number of blocks created by each BP consecutively during each round.
    - ``skipLastBlocks`` number of final blocks to skipped by each BP during each production interval to reduce stale blocks during hand-offs.
    - ``confirmations`` number of confirmation blocks required until transaction latency is determined.
    - ``randomShuffle`` true - order of BPs is random, false - order of BPs is determined according to travelling salesman.
    - ``consensusOnOrchestrator`` true - selected BPs are automatically moved to orchestrator. Recommended for fast block rates and coordinators without time synchronization.
    - ``peers`` list used to override default configuration of ``blockchainDefaults`` for specific peers (may be empty).
    
    ```yaml
    dPoSBFT:
      #consensusNodes: [1, 3, 6]    # list of ids
      consensusNodeNum: 21          # integer in [1,nodes]
      nodeSelection: best           # worst | random | best
      blocksPerNode: 180            # integer in [1,...]
      skipLastBlocks: 0             # integer in [0,...]
      confirmations: 1              # integer in [1,...]
      randomShuffle: false          # bool
      consensusOnOrchestrator: true # bool
      peers: 
        - id: 0
          txShare: 2 
          txSize: 100
          txFees: [{fee: 1, share: 1}, {fee: 2, share: 2}] 
          txPoolSize: 1000
          verificationTime: 5
          txDistribution: poisson # constant | uniform | poisson
        - id: 1
          txShare: 2
          txSize: 100
          txFees: [{fee: 1, share: 2}, {fee: 2, share: 3}]
          txPoolSize: 1000
          verificationTime: 5
        ...
    ```

### Miscellaneous
Specify logging and output parameters.
- ``logLevel`` controls console output verbosity.
- ``networkDelay`` time to wait for synchronization during startup/shutdown. Orchestrator will start connecting to remaining coordinators after [networkDelay] milliseconds.
- ``skipBlocks`` After start-up, not all mining threads may be running immediately. Use this parameter to discard the first couple blocks. Recommended for high block rates or heavy load on coordinators.
- ``runs`` number of repetitions for this experiment.
- ``manualStart`` true - instead of after [networkDelay] ms, orchestrator is started manually by sending new line to console.
- ``exportAsJson`` true - export results as JSON (as opposed to txt format).
- ``renderBlockchain``, ``renderGraph`` true - output Graphviz representations as svg. Not recommended for large networks and blockchains.
```yaml
logLevel: CONFIG        # ALL | FINEST | FINER | FINE | CONFIG | INFO | WARNING | SEVERE | OFF  
networkDelay: 5000      # integer in [0,...]
skipBlocks: 24          # integer in [0,...]
runs: 2                 # integer in [0,...]
manualStart: false      # bool
exportAsJson: false     # bool
renderBlockchain: false # bool
renderGraph: false      # bool
```

## Emulating Blockchain Networks
1. Write configuration files for the intended experiments and place them into a folder shared between all coordinators.
2. Start the coordinators:
    - Execute the jar file using: 
        ```shell script
        $java -jar -Xms4g -Xmx8g path/to/jar.jar <this coordinator's ip> <this coordinator's port> path/to/config/folder
        ```
    - After starting the orchestrator (first coordinator in config) the emulation will start within [networkDelay] ms. Therefore, starting the orchestrator last is recommended.
3. All config files in the folder are executed in alphabetical order, results are written to files on the orchestrator.
4. During emulation, all runs on all machines may be aborted by terminating (ctrl+C, ...) the process of any coordinator or the orchestrator. 
5. Coordinators terminate automatically after all configs and runs are completed.

## Results
Results are exported as JSON on the orchestrator with file name ``timestamp_configFileName_runCount.json`` and include a list of all transaction latencies.
```yaml
{
    "config": {object}, # copy of config.yaml as JSON
    "executionTime": 12.4, # minutes
    "cpuLoads": [{
        "coordinator": "localhost/127.0.0.1:5151",
        "maxCPULoad": 4.312, # %
        "avgCPULoad": 4.312 # %
    }],
    "avgBlockSize": 3.3, # transactions
    "fullBlocks": 0, # blocks
    "staleBlocks": 9,
    "forkLengths": [{
        "length": 1.0,
        "count": 7.0
    },{
        "length": 2.0,
        "count": 1.0
    }],
    "confirmedStaleBlocks": 9,
    "avgPeerPoolSize": 34.0, # transactions
    "avgPeerOrphans": 0.0,
    "avgBlocksCreatedPerPeerPerSec": 0.0043,
    "avgBlocksCreatedPerPeer": 0.316,
    "avgTxCreatedPerPeerPerSec": 0.065,
    "avgTxCreatedPerPeer": 0.566,
    "avgConfirmedBlocksPerPeerPerSec": 1.325,
    "avgConfirmedTxPerPeerPerSec": 4.858,
    "avgConfirmedBytePerPeerPerSec": 971.797,
    "avgTotalBlocksPerPeerPerSec": 1.325,
    "avgTotalTxPerPeerPerSec": 2.558,
    "avgUnconfirmedTxPerPeer": 0.0,
    "avgTxLat": 1511.705, # ms
    "medianTxLat": 1386.0,
    "minTxLat": 35.0,
    "maxTxLat": 3047.0,
    "avgTxSize": 200.0, # byte
    "avgTxFee": 3.0,
    "txFees": [{
        "fee": 3.0,
        "count": 1511.705
    }],
    "allTxLats": [1386.0, 1452.0, 2608.0, ...]
}
```


## Known Issues
- YAML anchors, references and extensions appear to be unsupported by the selected library.
- In case of a lost connection during emulation, the current run is aborted and results may be incomplete.
- If no peers are hosted on the orchestrator machine, results will not contain information about the created blockchain. This can be circumvented by placing a single peer with id 0, as well as transaction and block rate of 0, on the orchestrator to be used as an observer of the peer-to-peer network.  
- In case of larger emulations, JVM may run out of heap space or file descriptors needed to create more socket connections.
    - To increase initial or maximum available heap space use `-Xms` and `-Xmx` flags, i.e. 
    ```shell script
    $java -jar -Xms4g -Xmx8g path/to/jar.jar
    ```
    - To increase available file descriptors on linux, use 
    ```shell script
    $ulimit -n 100000
    ```
- Conducting larger emulations (high block or tx rate, many nodes or edges) may exhaust real-time capabilities of the host, leading to inaccurate results due to extensive multithreading. Use multiple coordinator machines and pay attention to maximum and average CPU loads included in results.
- At very high block rates, consensus nodes of DPoS-BFT require time synchronization in order to avoid overlapping block production intervals. In case coordinator machines **are not** time synchronized, set ``consensusOnOrchestrator: true`` in order to move all BPs to the orchestrator machine during init.