#!/usr/bin/python -tt
# -*- coding:utf-8 -*-
__Author__="(wgzhao,wgzhao@gmail.com)"
'''
this scripts providers a simple method that configure multiple nic bonding
It try using GUI ,then use command line method if failure
typical use:
bond.py --bond bond0 --ipaddr 192.168.1.1 --netmask 255.255.255.0 --gateway 192.168.1.254 --run eth0 eth1 
you can also use short options switch,like as
bond.py -b bond0 -p 192.168.1.1 -n 255.255.255.0 -g 192.168.1.254 -r eth0 eth1

NOTES: at default, this script can NOT write configure files really,it just print all files' contents that will write to 
if you want to write to files ,you MUST use --run options
'''
import os,sys
from optparse import OptionParser
import getopt
from  Tkinter import *
import tkSimpleDialog

confdir='/etc/sysconfig/network-scripts/'
modconf='/etc/modprobe.conf'
netconf='/etc/sysconfig/network'

class guiBond(tkSimpleDialog.Dialog):
    '''
    create bonding configure files with GUI
    '''
    def __init__(self,top,ct):
        self.root=top
        self.test=IntVar()
        #('display name','option switch','input area','default value')
        self.elelist= [("Bond name","bond",Entry(self.root),"bond0"),
                    ("NIC name,separator by space","nic",Entry(self.root),"eth0 eth1"),
                    ("IP Address","ipaddr",Entry(self.root),"192.168.1.2"),
                    ("netmask","netmask",Entry(self.root),"255.255.255.0"),
                    ("gateway","gateway",Entry(self.root),"192.168.1.254"),
                    ("bonding mode(0,1)","mode",Entry(self.root),"0"),
                    ("run","run",Checkbutton(self.root,text='write config',variable=self.test))]
                    
        index = 0
        for ele in self.elelist:
            Label(self.root,text=ele[0]).grid(row=index,sticky = E)
            t = ele[2]
            #self.t = Entry(self.root)
            t.grid(row=index,column=1,sticky = W)
            if ele[1] != "run":
                t.insert(0,ele[3])
                               
            index += 1        
   
        b = Button(self.root, text="OK", command=self.ok).grid(row=index+1,column=1,sticky=S)
        
    def ok(self):
        args=[]
        nics=[]
        for ele in self.elelist:
            t = ele[2]
            if ele[1] == "nic":
                nics=t.get().split()
            elif ele[1] == "run":
                if self.test.get() == 1:
                    args.append('--run')
            else:
                args.append('--%s' % ele[1])
                args.append(t.get())
            args += nics   
        #debug
        (options,args) = ct.parseargs(args=args)
        ct.writeconf(options,args)
        self.root.destroy()

class createBond:
    
    def __init__(self):
        self.parse = OptionParser(description='''
        configure necessary files about mulitiple NIC bonding
        nic1,nic2..nicN indicate your NIC adapter name which is eth0 in common,no given,we guess all nic name
        ''',
                         version='0.1',
                         usage='%prog [options ] [nic1,nic2...nicN]'
                    )
        self.parse.add_option('-b','--bond',dest='bond',help='the bond name,default is bond0',default='bond0')
        self.parse.add_option('-p','--ipaddr',dest='ipaddr',help='IP address,default is 172.16.80.15',default='172.16.80.15')
        self.parse.add_option('-n','--netmask',dest='netmask',help='the netmask,default is 255.255.255.0',default='255.255.255.0')
        self.parse.add_option('-g','--gateway',dest='gateway',help='the gateway of network,default is 172.16.80.1',default='172.16.80.1')
        self.parse.add_option('-m','--mode',dest='mode',type='int',help='the type of bond,0=round robin,1=active-backup. default is 0',default='0')
        self.parse.add_option('-r','--run',action='store_true',dest='run',help='run actively ,write configure file[default is not]',default=False)
       
        #the directory we create or modify files
          
    def usage(self):
        self.parse.print_help()
        
            
    def parseargs(self,**pars):
        '''
        configure multiple nics bonding config files
        '''
        if len(pars) > 0:
            (options,args)=self.parse.parse_args(args=pars['args'])
        else:
            (options,args) = self.parse.parse_args()
        return (options,args)
        
    def writeconf(self,options,args):
        data = {}
        data[modconf] = "alias %s bonding \noptions %s miimon=5 mode=%d \n" % (options.bond,options.bond,options.mode)
        data[netconf] = 'GATEWAY=%s \n' % options.gateway
        data[''.join([confdir,'ifcfg-',options.bond])] = 'DEVICE=%s \nIPADDR=%s \nNETMASK=%s \nONBOOT=yes \nBOOTPROTO=none \n' % (options.bond,options.ipaddr,options.netmask)
        for nic in args:
            data[''.join([confdir,'ifcfg-',nic])] = 'DEVICE=%s \nUSERCTL=no \nONBOOT=yes \nMASTER=%s \nSLAVE=yes \nBOOTPROTO=none \n' % (nic,options.bond)
       
        if not options.run :
            for k,v in data.items():
                print 'write/append: ',k
                print v
                print '-' * 20
            return 0
        for k,v in data.items():
            try:
                open(k).write(v)
            except IOError,err:
                print str(err)
                return 1
                
                
  
if __name__ == "__main__":
    
    '''if (os.getuid() > 0):
        print 'the script needs root account to execute it'
        sys.exit(2)
    '''
    ct =createBond()
    
    if len(sys.argv) and ('--help' in sys.argv[1:] or '-h' in sys.argv[1:]):
        ct.usage()
        sys.exit(0)
    #debug
    try:
        root = Tk()
        root.title('NIC bonding util')
        d = guiBond(root,ct)
        root.mainloop()
    except:    
        (options,args)=ct.parseargs()
        ct.writeconf(options,args)
