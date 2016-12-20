package systemtests

import (
	"fmt"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/contiv/contivmodel/client"
	. "gopkg.in/check.v1"
)

func (s *systemtestSuite) TestBasicStartRemoveContainerVXLAN(c *C) {
	s.testBasicStartRemoveContainer(c, "vxlan")
}

func (s *systemtestSuite) TestBasicStartRemoveContainerVLAN(c *C) {
	s.testBasicStartRemoveContainer(c, "vlan")
}

func (s *systemtestSuite) testBasicStartRemoveContainer(c *C, encap string) {

	if s.fwdMode == "routing" && encap == "vlan" {
		s.SetupBgp(c, false)
		s.CheckBgpConnection(c)
	}
	c.Assert(s.cli.NetworkPost(&client.Network{
		PktTag:      1001,
		NetworkName: "private",
		Subnet:      "10.1.0.0/16",
		Gateway:     "10.1.1.254",
		Encap:       encap,
		TenantName:  "default",
	}), IsNil)

	for i := 0; i < s.basicInfo.Iterations; i++ {
		containers, err := s.runContainers(s.basicInfo.Containers, false, "private", "", nil, nil)
		c.Assert(err, IsNil)

		if s.fwdMode == "routing" && encap == "vlan" {
			var err error
			err = s.CheckBgpRouteDistribution(c, containers)
			c.Assert(err, IsNil)
		}

		c.Assert(s.pingTest(containers), IsNil)
		c.Assert(s.removeContainers(containers), IsNil)
	}

	c.Assert(s.cli.NetworkDelete("default", "private"), IsNil)
}

func (s *systemtestSuite) TestBasicStartStopContainerVXLAN(c *C) {
	s.testBasicStartStopContainer(c, "vxlan")
}

func (s *systemtestSuite) TestBasicStartStopContainerVLAN(c *C) {
	s.testBasicStartStopContainer(c, "vlan")
}

func (s *systemtestSuite) testBasicStartStopContainer(c *C, encap string) {
	if s.fwdMode == "routing" && encap == "vlan" {
		s.SetupBgp(c, false)
		s.CheckBgpConnection(c)
	}
	c.Assert(s.cli.NetworkPost(&client.Network{
		PktTag:      1001,
		NetworkName: "private",
		Subnet:      "10.1.0.0/16",
		Gateway:     "10.1.1.254",
		Encap:       encap,
		TenantName:  "default",
	}), IsNil)

	containers, err := s.runContainers(s.basicInfo.Containers, false, "private", "", nil, nil)
	c.Assert(err, IsNil)
	if s.fwdMode == "routing" && encap == "vlan" {
		var err error
		err = s.CheckBgpRouteDistribution(c, containers)
		c.Assert(err, IsNil)
	}

	for i := 0; i < s.basicInfo.Iterations; i++ {
		c.Assert(s.pingTest(containers), IsNil)

		errChan := make(chan error)
		for _, cont := range containers {
			go func(cont *container) { errChan <- cont.node.exec.stop(cont) }(cont)
		}

		for range containers {
			c.Assert(<-errChan, IsNil)
		}

		for _, cont := range containers {
			go func(cont *container) { errChan <- cont.node.exec.start(cont) }(cont)
		}

		for range containers {
			c.Assert(<-errChan, IsNil)
		}

		if s.fwdMode == "routing" && encap == "vlan" {
			var err error
			err = s.CheckBgpRouteDistribution(c, containers)
			c.Assert(err, IsNil)
		}

	}

	c.Assert(s.removeContainers(containers), IsNil)
	c.Assert(s.cli.NetworkDelete("default", "private"), IsNil)
}

func (s *systemtestSuite) TestBasicNameServerVXLAN(c *C) {
	if s.basicInfo.Scheduler == "k8" {
		return
	}
	s.testBasicNameServer(c, "vxlan")
}

func (s *systemtestSuite) TestBasicNameServerVLAN(c *C) {
	if s.basicInfo.Scheduler == "k8" {
		return
	}
	s.testBasicNameServer(c, "vlan")
}

func (s *systemtestSuite) testBasicNameServer(c *C, encap string) {

        epg_name1 := "contiv-epg1"
        epg_name2 := "contiv-epg2"

	if s.fwdMode == "routing" && encap == "vlan" {
		s.SetupBgp(c, false)
		s.CheckBgpConnection(c)
	}

	c.Assert(s.cli.NetworkPost(&client.Network{
		PktTag:      1001,
		NetworkName: "private",
		Subnet:      "10.1.1.0/24",
		Gateway:     "10.1.1.254",
		Encap:       encap,
		TenantName:  "default",
	}), IsNil)

	for i := 0; i < s.basicInfo.Iterations; i++ {
		group1 := &client.EndpointGroup{
			GroupName:   fmt.Sprintf("%s%d", epg_name1, i),
			NetworkName: "private",
			Policies:    []string{},
			TenantName:  "default",
		}
		group2 := &client.EndpointGroup{
			GroupName:   fmt.Sprintf("%s%d", epg_name2, i),
			NetworkName: "private",
			Policies:    []string{},
			TenantName:  "default",
		}
		logrus.Infof("Creating epg: %s", group1.GroupName)
		c.Assert(s.cli.EndpointGroupPost(group1), IsNil)
		logrus.Infof("Creating epg: %s", group2.GroupName)
		c.Assert(s.cli.EndpointGroupPost(group2), IsNil)


		containers1, err := s.runContainersWithDNS(s.basicInfo.Containers, "default", "private",
                        fmt.Sprintf("%s%d", epg_name1,i), "rchirakk/ubuntu-dns")
		c.Assert(err, IsNil)
		containers2, err := s.runContainersWithDNS(s.basicInfo.Containers, "default", "private",
                        fmt.Sprintf("%s%d", epg_name2, i), "rchirakk/ubuntu-dns")
		c.Assert(err, IsNil)

		containers := append(containers1, containers2...)
		if s.fwdMode == "routing" && encap == "vlan" {
			var err error
			err = s.CheckBgpRouteDistribution(c, containers)
			c.Assert(err, IsNil)
		}
		if s.fwdMode == "routing" && encap == "vlan" {
			time.Sleep(5 * time.Second)
		}

                logrus.Infof("cont1: %+v", containers1)
                logrus.Infof("cont2: %+v", containers2)

		// Check epg name resolution

		c.Assert(s.pingTestByName(containers, fmt.Sprintf("%s%d", epg_name1, i)), IsNil)
		c.Assert(s.pingTestByName(containers, fmt.Sprintf("%s%d", epg_name2, i)), IsNil)

                // check container name name
                c.Assert(s.pingTestByName(containers2, containers2[0].name), IsNil)
                c.Assert(s.pingTestByName(containers1, containers1[0].name), IsNil)

		// cleanup
		c.Assert(s.removeContainers(containers), IsNil)
		c.Assert(s.cli.EndpointGroupDelete(group1.TenantName, group1.GroupName), IsNil)
		c.Assert(s.cli.EndpointGroupDelete(group2.TenantName, group2.GroupName), IsNil)
	}

	c.Assert(s.cli.NetworkDelete("default", "private"), IsNil)
}
