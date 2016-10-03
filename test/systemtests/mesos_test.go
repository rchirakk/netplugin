package systemtests

import (
	"bytes"
	"encoding/json"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"io/ioutil"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"time"
)

var sysTestLog = log.New()
var mesos_master *node
var marathonIP string

type mesosSysTestScheduler struct {
	mesosSysTestsNode *node
}

type sysLogFmt struct{}

func (t *sysLogFmt) Format(e *log.Entry) ([]byte, error) {
	e.Message = strings.Join([]string{"[MESOS-SYSTEST]", e.Message}, " ")
	nt := log.TextFormatter{}
	return nt.Format(e)
}

func (s *systemtestSuite) NewMesosExec(n *node) *mesosSysTestScheduler {
	mesosScheduler := new(mesosSysTestScheduler)
	mesosScheduler.mesosSysTestsNode = n
	sysTestLog.Formatter = new(sysLogFmt)
	if strings.Contains(n.Name(), "node1") {
		mesos_master = n
		if mip, err := mesos_master.getIPAddr("eth1"); err != nil {
			log.Errorf("failed to get marathon ip address")
			return nil
		} else {
			marathonIP = mip
		}
	}

	return mesosScheduler
}

type marathonLabels struct {
	Tenant   string `json:"io.contiv.tenant,omitempty"`
	Network  string `json:"io.contiv.network,omitempty"`
	NetGroup string `json:"io.contiv.net-group,omitempty"`
}

type marathonIpAddr struct {
	NetworkName string         `json:"networkName"`
	Labels      marathonLabels `json:"labels"`
}

type marathonMesos struct {
	Image          string   `json:"image"`
	Privileged     bool     `json:"privileged"`
	Parameters     []string `json:"parameters"`
	ForcePullImage bool     `json:"forcePullImage"`
}

type marathonContainer struct {
	Type    string        `json:"type"`
	volumes []string      `json:"volumes"`
	Mm      marathonMesos `json:"mesos"`
}

type marathonJob struct {
	Id        string            `json:"id"`
	Cmd       string            `json:"cmd"`
	Cpus      int               `json:"cpus"`
	Mem       int               `json:"mem"`
	Disk      int               `json:"disk"`
	Instances int               `json:"instances"`
	Mc        marathonContainer `json:"container"`
	MIPAddr   marathonIpAddr    `json:"ipAddress"`
}

type taskIpAddr struct {
	IpAddress string `json:"ipAddress"`
	Protocol  string `json:"protocol"`
}

type taskResp struct {
	Id     string       `json:"id"`
	State  string       `json:"state"`
	IpAddr []taskIpAddr `json:"ipAddresses"`
}

type marathonRespApp struct {
	Id    string     `json:"id"`
	Tasks []taskResp `json:"tasks"`
}

type marathonResp struct {
	App  marathonRespApp   `json:"app"`
	Apps []marathonRespApp `json:"apps"`
}

var jobid = 1001

func assertOnError(err error, msg string) bool {
	if err != nil {
		log.Errorf("%s:%s", msg, err)
		return true
	}
	return false
}

func processHttpPost(url string, jReq []byte) error {

	log.Infof("posting to url %s", url)
	httpResp, err := http.Post(url, "application/json", bytes.NewBuffer(jReq))
	if err != nil {
		log.Errorf("failed in http POST %s", err)
		return err
	}

	defer httpResp.Body.Close()

	switch httpResp.StatusCode {

	case http.StatusOK:
	case http.StatusCreated:
		info, err := ioutil.ReadAll(httpResp.Body)
		if err != nil {
			log.Errorf("failed to get http data: %s", err)
			return err
		}
		log.Infof("http response: %s", string(info))

	default:
		log.Errorf("received unknown http error %d ", httpResp.StatusCode)
		info, err := ioutil.ReadAll(httpResp.Body)
		if err != nil {
			log.Errorf("failed to get http data: %s", err)
			return err
		}
		log.Errorf("error: %s", string(info))
		return fmt.Errorf("invalid status code")
	}

	return nil
}

func processHttpGet(url string) ([]byte, error) {
	log.Infof("get from url %s", url)
	httpResp, err := http.Get(url)
	if err != nil {
		log.Errorf("failed in http GET: %s", err)
		return nil, err
	}

	defer httpResp.Body.Close()

	switch httpResp.StatusCode {

	case http.StatusOK:
		info, err := ioutil.ReadAll(httpResp.Body)
		if err != nil {
			log.Errorf("failed to get http data: %s", err)
			return nil, err
		}
		log.Infof("http response: %s", string(info))
		return info, nil

	default:
		log.Errorf("received unknown http error %d ", httpResp.StatusCode)
		info, err := ioutil.ReadAll(httpResp.Body)
		if err != nil {
			log.Errorf("failed to get http data: %s", err)
			return nil, err
		}
		log.Errorf("error: %s", string(info))
		return nil, fmt.Errorf("invalid status code")
	}

	return nil, nil
}

func processHttpDel(url string) ([]byte, error) {
	log.Infof("del from url %s", url)
	httpReq, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		log.Errorf("failed in creating DEL req: %s", err)
		return nil, err
	}

	httpResp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		log.Errorf("failed in http DEL: %s", err)
		return nil, err
	}

	defer httpResp.Body.Close()

	switch httpResp.StatusCode {

	case http.StatusOK:
		info, err := ioutil.ReadAll(httpResp.Body)
		if err != nil {
			log.Errorf("failed to get http data: %s", err)
			return nil, err
		}
		log.Infof("http response: %s", string(info))
		return info, nil

	default:
		log.Errorf("received unknown http error %d ", httpResp.StatusCode)
		info, err := ioutil.ReadAll(httpResp.Body)
		if err != nil {
			log.Errorf("failed to get http data: %s", err)
			return nil, err
		}
		log.Errorf("error: %s", string(info))
		return nil, fmt.Errorf("invalid status code")
	}

	return nil, nil
}

func (ms1 *mesosSysTestScheduler) runContainer(spec containerSpec) (*container, error) {
	mesosContainer := marathonJob{
		Cmd:       "sleep 300000",
		Cpus:      1,
		Mem:       500,
		Disk:      0,
		Instances: 1,
		Mc: marathonContainer{
			Type: "MESOS",
			Mm: marathonMesos{
				Image:          "contiv/alpine",
				Privileged:     false,
				ForcePullImage: true,
			},
		},
		MIPAddr: marathonIpAddr{
			NetworkName: "netcontiv",
		},
	}

	if spec.tenantName != "default" {
		mesosContainer.MIPAddr.Labels.Tenant = spec.tenantName
	}

	mesosContainer.MIPAddr.Labels.NetGroup = spec.epGroup
	mesosContainer.MIPAddr.Labels.Network = spec.networkName

	if len(spec.commandName) > 0 {
		mesosContainer.Cmd = spec.commandName
	}
	if len(spec.imageName) > 0 {
		mesosContainer.Mc.Mm.Image = spec.imageName
	}

	jobid++
	mesosContainer.Id = strconv.Itoa(jobid)

	log.Infof("creating container %s id %d", spec.name, jobid)

	jReq, err := json.Marshal(mesosContainer)
	assertOnError(err, "json marshal")
	if err != nil {
		return nil, err
	}

	if err := processHttpPost("http://"+marathonIP+":8080/v2/apps", jReq); err != nil {
		return nil, err
	}

	// check status
	mResp := marathonResp{}

	for l := 0; l < 120; l++ {

		jResp, err := processHttpGet("http://" +
			marathonIP + ":8080/v2/apps/" + mesosContainer.Id)
		if err != nil {
			return nil, err
		}

		log.Infof("http get loop count: %d ", l+1)

		if err := json.Unmarshal(jResp, &mResp); err != nil {
			log.Errorf("failed to unmarshal responase")
			return nil, err
		}

		if len(mResp.App.Tasks) <= 0 || mResp.App.Tasks[0].State == "TASK_STAGING" {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		log.Infof("task state %s", mResp.App.Tasks[0].State)

		if mResp.App.Tasks[0].State == "TASK_RUNNING" {
			break
		} else {
			log.Errorf("invalid task state %s", mResp.App.Tasks[0].State)
			return nil, fmt.Errorf("invalid task state")
		}
	}

	if mResp.App.Tasks[0].State != "TASK_RUNNING" {
		log.Errorf("exhausted loop, bailing out")
		return nil, fmt.Errorf("task is not running")
	}

	mc := &container{}
	mc.containerID = mResp.App.Tasks[0].Id
	mc.name = mesosContainer.Id
	mc.node = mesos_master

	for j := 0; j < len(mResp.App.Tasks[0].IpAddr); j++ {
		if mResp.App.Tasks[0].IpAddr[j].Protocol == "IPv4" {
			mc.eth0.ip = mResp.App.Tasks[0].IpAddr[j].IpAddress
		} else if mResp.App.Tasks[0].IpAddr[j].Protocol == "IPv6" {
			mc.eth0.ipv6 = mResp.App.Tasks[0].IpAddr[j].IpAddress
		}
	}
	log.Infof("container info %+v", mc)
	return nil, nil
}

func unknown() {
	log.Infof("PPPPPPPPPPPPPPP launching to MARS PPPPPPPPPPPPPP")
	if pc, _, _, ok := runtime.Caller(1); ok {
		f := runtime.FuncForPC(pc)
		log.Infof("==== %s()", f.Name())
	}
}

func (ms1 *mesosSysTestScheduler) stop(c *container) error {
	unknown()
	log.Infof("===== %+v", ms1)
	return nil
}

func (ms1 *mesosSysTestScheduler) start(c *container) error {
	unknown()
	log.Infof("===== %+v", ms1)
	return nil
}

func (ms1 *mesosSysTestScheduler) startNetmaster() error {
	log.Infof("Starting netmaster on %s", ms1.mesosSysTestsNode.Name())
	dnsOpt := " --dns-enable=false "
	if ms1.mesosSysTestsNode.suite.enableDNS {
		dnsOpt = " --dns-enable=true "
	}
	return ms1.mesosSysTestsNode.tbnode.RunCommandBackground(ms1.mesosSysTestsNode.suite.binpath +
                "/netmaster" + dnsOpt + " --cluster-store " +
                ms1.mesosSysTestsNode.suite.clusterStore + " &> /tmp/netmaster.log")
}

func (ms1 *mesosSysTestScheduler) stopNetmaster() error {
	log.Infof("Stopping netmaster on %s", ms1.mesosSysTestsNode.Name())
	return ms1.mesosSysTestsNode.tbnode.RunCommand("sudo pkill netmaster")
}

func (ms1 *mesosSysTestScheduler) stopNetplugin() error {
	log.Infof("Stopping netplugin on %s", ms1.mesosSysTestsNode.Name())
	return ms1.mesosSysTestsNode.tbnode.RunCommand("sudo pkill netplugin")
}

func (ms1 *mesosSysTestScheduler) startNetplugin(args string) error {
	log.Infof("Starting netplugin on %s", ms1.mesosSysTestsNode.Name())
	return ms1.mesosSysTestsNode.tbnode.RunCommandBackground("sudo " +
                ms1.mesosSysTestsNode.suite.binpath + "/netplugin -plugin-mode docker -vlan-if " +
                ms1.mesosSysTestsNode.suite.vlanIf + " --cluster-store " +
                ms1.mesosSysTestsNode.suite.clusterStore + " " + args + "&> /tmp/netplugin.log")

}

func (ms1 *mesosSysTestScheduler) cleanupContainers() error {

	mResp := marathonResp{}
	jResp, err := processHttpGet("http://" +
		marathonIP + ":8080/v2/apps/")
	if err != nil {
		return err
	}

	if err := json.Unmarshal(jResp, &mResp); err != nil {
		log.Errorf("failed to unmarshal response")
		return err
	}

	failed := false

	for i := 0; i < len(mResp.Apps); i++ {
		log.Infof("deleteing container %s", mResp.Apps[i].Id)
		if _, err := processHttpDel("http://" + marathonIP +
			":8080/v2/apps" + mResp.Apps[i].Id); err != nil {
			log.Errorf("failed to delete %s", mResp.Apps[i].Id)
			failed = failed || true

		}
	}

	if failed {
		return fmt.Errorf("failed to cleanup containers")

	} else {
		return nil

	}
}

func (ms1 *mesosSysTestScheduler) checkNoConnection(c *container, ipaddr, protocol string, port int) error {
	unknown()
	return nil
}

func (ms1 *mesosSysTestScheduler) checkConnection(c *container, ipaddr, protocol string, port int) error {
	unknown()
	return nil
}

func (ms1 *mesosSysTestScheduler) startListener(c *container, port int, protocol string) error {
	unknown()
	return nil
}

func (ms1 *mesosSysTestScheduler) rm(c *container) error {
	unknown()
	return nil
}

func (ms1 *mesosSysTestScheduler) getIPAddr(c *container, dev string) (string, error) {
	unknown()
	return "", nil
}
func (ms1 *mesosSysTestScheduler) checkPing(c *container, ipaddr string) error {
	unknown()
	return nil
}
func (ms1 *mesosSysTestScheduler) checkPing6(c *container, ipv6addr string) error {
	unknown()
	return nil
}
func (ms1 *mesosSysTestScheduler) checkPingFailure(c *container, ipaddr string) error {
	unknown()
	return nil
}
func (ms1 *mesosSysTestScheduler) checkPing6Failure(c *container, ipv6addr string) error {
	unknown()
	return nil
}
func (ms1 *mesosSysTestScheduler) cleanupSlave() {
        log.Infof("Cleaning up slave on %s", ms1.mesosSysTestsNode.Name())
	vNode := ms1.mesosSysTestsNode.tbnode
	vNode.RunCommand("sudo ovs-vsctl del-br contivVxlanBridge")
	vNode.RunCommand("sudo ovs-vsctl del-br contivVlanBridge")
	vNode.RunCommand("for p in `ifconfig  | grep vport | " +
                "awk '{print $1}'`; do sudo ip link delete $p type veth; done")
	vNode.RunCommand("sudo rm /var/run/docker/plugins/netplugin.sock")
	vNode.RunCommand("sudo service docker restart")
}
func (ms1 *mesosSysTestScheduler) cleanupMaster() {
	log.Infof("Cleaning up master on %s", ms1.mesosSysTestsNode.Name())
	vNode := ms1.mesosSysTestsNode.tbnode
	vNode.RunCommand("etcdctl rm --recursive /contiv")
	vNode.RunCommand("etcdctl rm --recursive /contiv.io")
	vNode.RunCommand("etcdctl rm --recursive /docker")
	vNode.RunCommand("etcdctl rm --recursive /skydns")
	vNode.RunCommand("curl -X DELETE localhost:8500/v1/kv/contiv.io?recurse=true")
	vNode.RunCommand("curl -X DELETE localhost:8500/v1/kv/docker?recurse=true")
}
func (ms1 *mesosSysTestScheduler) runCommandUntilNoNetpluginError() error {
	return ms1.mesosSysTestsNode.runCommandUntilNoError("pgrep netplugin")
}

func (ms1 *mesosSysTestScheduler) runCommandUntilNoNetmasterError() error {
	return ms1.mesosSysTestsNode.runCommandUntilNoError("pgrep netmaster")
}

func (ms1 *mesosSysTestScheduler) rotateNetmasterLog() error {
        fn := "netmaster.log"
        _, err := ms1.mesosSysTestsNode.runCommand(fmt.Sprintf("mv %s %s`date +%%s`", fn, fn + ".old-"))
	return err
}

func (ms1 *mesosSysTestScheduler) rotateNetpluginLog() error {
        fn := "netplugin.log"
        _, err := ms1.mesosSysTestsNode.runCommand(fmt.Sprintf("mv %s %s`date +%%s`", fn, fn + ".old-"))
	return err
}

func (ms1 *mesosSysTestScheduler) getIPv6Addr(c *container, dev string) (string, error) {
	unknown()
	return "", nil
}

func (ms1 *mesosSysTestScheduler) checkForNetpluginErrors() error {
	out, _ := ms1.mesosSysTestsNode.tbnode.
                         RunCommandWithOutput(`for i in /tmp/net*; do grep -A 5 "panic\|fatal" $i; done`)
	if out != "" {
		log.Errorf("Fatal error in logs on %s: \n", ms1.mesosSysTestsNode.Name())
		fmt.Printf("%s\n==========================================\n", out)
		return fmt.Errorf("fatal error in netplugin logs")
	}

	out, _ = ms1.mesosSysTestsNode.tbnode.RunCommandWithOutput(`for i in /tmp/net*; do grep "error" $i; done`)
	if out != "" {
		log.Errorf("error output in netplugin logs on %s: \n", ms1.mesosSysTestsNode.Name())
		fmt.Printf("%s==========================================\n\n", out)

	}

	return nil
}

func (ms1 *mesosSysTestScheduler) rotateLog(prefix string) error {
	unknown()
	return nil
}

func (ms1 *mesosSysTestScheduler) checkConnectionRetry(c *container, ipaddr, protocol string, port, delay, retries int) error {
	unknown()
	return nil
}

func (ms1 *mesosSysTestScheduler) checkNoConnectionRetry(c *container, ipaddr, protocol string, port, delay, retries int) error {
	unknown()
	return nil
}

func (ms1 *mesosSysTestScheduler) checkPingWithCount(c *container, ipaddr string, count int) error {
	unknown()
	return nil
}
func (ms1 *mesosSysTestScheduler) checkPing6WithCount(c *container, ipaddr string, count int) error {
	unknown()
	return nil
}
func (ms1 *mesosSysTestScheduler) checkSchedulerNetworkCreated(nwName string, expectedOp bool) error {
	unknown()
	return nil
}
func (ms1 *mesosSysTestScheduler) waitForListeners() error {
	unknown()
	return nil
}

func (ms1 *mesosSysTestScheduler) verifyVTEPs(expVTEPS map[string]bool) (string, error) {
	unknown()
	return "", nil
}
func (ms1 *mesosSysTestScheduler) verifyEPs(epList []string) (string, error) {
	unknown()
	return "", nil
}

func (ms1 *mesosSysTestScheduler) reloadNode(n *node) error {
	unknown()
	return nil
}

func (ms1 *mesosSysTestScheduler) getMasterIP() (string, error) {
	unknown()
	return "", nil
}

func (ms1 *mesosSysTestScheduler) startIperfServer(containers *container) error {
	unknown()
	return nil
}

func (ms1 *mesosSysTestScheduler) startIperfClient(containers *container, ip, limit string, isErr bool) error {
	unknown()
	return nil
}

func (ms1 *mesosSysTestScheduler) tcFilterShow(bw string) error {
	unknown()
	return nil
}
