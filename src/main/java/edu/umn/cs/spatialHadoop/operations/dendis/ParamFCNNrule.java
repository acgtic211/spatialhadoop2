package edu.umn.cs.spatialHadoop.operations.dendis;

public class ParamFCNNrule {
  double[] Cost_n;
  double[] d_1knn;//liste des max.
  double[] d_1knnmin;//liste des min
  //double[] Ave_d1nn_proto;
  double[][] pattern;
  double[] dav_1knn;//liste des moyennes des distances
  double Seuil_massique;
  double DensiteMin;
  double Thres_d_1knn;
  double Thres_Wt_1knn;
  double Thres_EcartW;
  double Thres_SequenceW;
  double Thres_SequenceD;
  double Ratio_distance;
  double Thres_FreqW;
  double Thres_RatioKmin;
  double Thres_RatioKmax;
  double Thres_EcartN;
  int NbPointsMin;
  int NbPointsNoise;
  double OccMin;
  double V_seuilH, V_seuilB;
  double C_seuilH, C_seuilB;
  double Coef;
  int SansStep2;
  int Auto;
  int Option_Alea_step2;
  int Option_Alea_Bruit;
  int Option_Alea_Cleaning_1;
  int Option_Alea_Cleaning_2;
  int Option_Assymptotique;
  int Init_Aleatoire;
  int n,p;
  int MaxCluster;
  int L_pattern;
  int Save;
  double Epsilon_Assymptote;
  double Epsilon_R;
  int Plage_R;
  double Dist_Noise;
  double Dist_Connexity;
  int Barycentre;
  int MaxClusterMemory;
  double[] Nb_distance = new double[1000];
  double[] Nb_TriangularTest = new double[1000];
  double Granularity;
  int Mass_WeightCriterion;
  int Mass_OptionFinal;
  int ActifMasse;
  double TradeoffDensite;
  double FactorDistance;
  int S2_Option_all;
  double S2_Beta;
  int Ratio_achieved;
  int Tha_achieved;
  //double (*fdist)(double  *,double  *,int n);
  public ParamFCNNrule(int n, int p){
    this.n = n;
    int pmax = p;
    //this.fdist = d_Eucl;
    //this.Ave_d1nn_proto = new double[n];
    this.d_1knn = new double[n];
    this.d_1knnmin = new double[n];//liste des min
    this.dav_1knn = new double[n];//liste des moyennes des distances
    this.Cost_n = new double[n];
    //this.pattern = Utils.Reservmemdouble(n,pmax);
    this.MaxClusterMemory = n;

    for (int i =0;i<n;i++)
    {
      this.Cost_n[i] = Double.MAX_VALUE;
      this.d_1knn[i] =0;
      this.d_1knnmin[i] = 0;
      this.dav_1knn[i] = 0;
      //this.Ave_d1nn_proto[i] = 0;
    }
    this.MaxCluster = n;

    for (int i=0;i<1000;i++)
    {
      this.Nb_distance[i] = 0;
      this.Nb_TriangularTest[i] = 0;
    }
  }

  void InitSingleParamFCNNrule()
  {
    this.SansStep2 = 0;
    this.TradeoffDensite = 2;
    this.FactorDistance = 1;
    this.Thres_RatioKmin = 0.9;
    this.Thres_RatioKmax = 0.9;
    this.Thres_d_1knn =  15.;
    this.Thres_Wt_1knn = 10.;//Percentange
    this.Thres_SequenceD = 15;
    this.Thres_SequenceW = 5;
    this.Thres_EcartN = 90;
    this.Thres_EcartW= 5.;//Delta des inerties du step t � t+1.
    this.Thres_FreqW = 0.9;
    this.Ratio_distance = 0.5;
    this.Coef = 0.8;
    this.V_seuilH = 50;
    this.C_seuilH = 2;
    this.V_seuilB = 20;
    this.C_seuilB = 1;
    this.Option_Alea_Cleaning_1 = 0;
    this.Option_Alea_Cleaning_2 = 0;
    this.Option_Alea_Bruit = 0;
    this.Option_Alea_step2 = 1;
    this.Init_Aleatoire = 1;
    this.Seuil_massique = 0.2;
    this.Option_Assymptotique = 0;
    this.DensiteMin=10;
    this.NbPointsMin=1;
    this.Granularity = 0.01;
    this.NbPointsNoise=0;
    this.OccMin = 10;
    this.Auto = 0;
    this.L_pattern = 1;
    this.Save = 0;
    this.p = 1;
    this.Epsilon_Assymptote = 0.001;
    this.Epsilon_R = 0.01;
    this.Plage_R = 5;
    this.Dist_Noise = 1.25;
    this.Dist_Connexity = 1.1;
    this.Barycentre = 0;
    this.Option_Alea_Bruit = 1;
    this.Mass_WeightCriterion = 1;
    this.Mass_OptionFinal = 1;
    this.ActifMasse = 0;
    this.S2_Option_all = 0;
    this.S2_Beta = 1.5;
    this.Ratio_achieved = 0;
    this.Tha_achieved = 0;
  }

  public double fdist(double[] i1, double[] i2, int n) {
    int i;
    double d = 0;

    for (i=0;i<n;i++)
      d = d + (i1[i] - i2[i])*(i1[i] - i2[i]);

    if (d!= 0.0)
    {
      d = Math.sqrt(d);
      return d;
    }
    else
      return 0;
  }
}
