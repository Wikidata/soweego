mkdir less-setup
cd less-setup

wget http://ftp.gnu.org/gnu/less/less-530.tar.gz
tar xzf less-530.tar.gz

cd less-530
sh configure && make && make install

cd ../..
rm -R -f less-setup